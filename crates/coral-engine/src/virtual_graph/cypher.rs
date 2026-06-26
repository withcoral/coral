use decypher::ast::clause::{Match, ProjectionItem, Return, SortDirection};
use decypher::ast::expr::{
    BinaryOperator as CypherBinaryOperator, ComparisonOperator as CypherComparisonOperator,
    Expression, Literal as CypherLiteral, NumberLiteral, UnaryOperator,
};
use decypher::ast::names::Variable;
use decypher::ast::pattern::{
    LabelExpression, NodePattern as CypherNodePattern, PatternElement,
    RelationshipDirection as CypherRelationshipDirection,
    RelationshipPattern as CypherRelationshipPattern,
};
use decypher::ast::query::{
    Query, QueryBody, ReadingClause, SinglePartBody, SinglePartQuery, SingleQueryKind,
};

use super::diagnostic::Diagnostic;
use super::ir::{
    ComparisonOperator, Direction, GraphPlan, Literal, NodePattern, OrderDirection, OrderKey,
    Projection, PropertyPredicate, PropertyRef, RelationshipPattern,
};
use crate::CoreError;

/// Parses and compiles the Coral-supported read-only Cypher subset into a shared graph plan.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed or uses
/// Cypher/GQL features outside Coral's current read-only virtual graph subset.
pub fn compile_cypher(cypher: &str) -> Result<GraphPlan, CoreError> {
    let query = decypher::parse(cypher).map_err(|error| {
        Diagnostic::new("CYPHER_PARSE_ERROR", "query", error.to_string()).into_core_error()
    })?;
    compile_query(&query)
}

fn compile_query(query: &Query) -> Result<GraphPlan, CoreError> {
    if query.statements.len() != 1 {
        return Err(unsupported(
            "query",
            "only a single Cypher statement is supported",
        ));
    }
    let statement = query
        .statements
        .first()
        .ok_or_else(|| unsupported("query", "Cypher query must contain a statement"))?;

    let QueryBody::SingleQuery(single_query) = statement else {
        return Err(unsupported(
            "query",
            "only read-only single MATCH queries are supported",
        ));
    };
    let SingleQueryKind::SinglePart(single_part) = &single_query.kind else {
        return Err(unsupported(
            "query",
            "WITH, UNION, and multi-part queries are not supported yet",
        ));
    };

    compile_single_part(single_part)
}

fn compile_single_part(query: &SinglePartQuery) -> Result<GraphPlan, CoreError> {
    let match_clause = match query.reading_clauses.as_slice() {
        [ReadingClause::Match(match_clause)] => match_clause,
        [] => {
            return Err(unsupported(
                "match",
                "exactly one MATCH clause is required before RETURN",
            ));
        }
        [ReadingClause::Unwind(_)] => {
            return Err(unsupported("match", "UNWIND is not supported"));
        }
        [ReadingClause::InQueryCall(_) | ReadingClause::CallSubquery(_)] => {
            return Err(unsupported("match", "CALL is not supported"));
        }
        [ReadingClause::LoadCsv(_)] => {
            return Err(unsupported("match", "LOAD CSV is not supported"));
        }
        _ => {
            return Err(unsupported(
                "match",
                "only one MATCH clause is supported per query",
            ));
        }
    };

    let return_clause = match &query.body {
        SinglePartBody::Return(return_clause) => return_clause,
        SinglePartBody::Updating { .. } => {
            return Err(unsupported(
                "query",
                "write clauses are not supported by Coral virtual graphs",
            ));
        }
        SinglePartBody::Finish(_) => {
            return Err(unsupported(
                "query",
                "FINISH is not supported because virtual graph queries must return rows",
            ));
        }
    };

    let mut plan = compile_match(match_clause)?;
    if let Some(where_clause) = &match_clause.where_clause {
        append_predicates(where_clause, &mut plan.predicates, "where")?;
    }
    compile_return(return_clause, &mut plan)?;
    Ok(plan)
}

fn compile_match(match_clause: &Match) -> Result<GraphPlan, CoreError> {
    if match_clause.optional {
        return Err(unsupported("match", "OPTIONAL MATCH is not supported yet"));
    }

    let pattern_part = match match_clause.pattern.parts.as_slice() {
        [pattern_part] => pattern_part,
        [] => {
            return Err(unsupported(
                "match.pattern",
                "MATCH pattern must not be empty",
            ));
        }
        _ => {
            return Err(unsupported(
                "match.pattern",
                "comma-separated pattern parts are not supported yet",
            ));
        }
    };
    if pattern_part.variable.is_some() {
        return Err(unsupported(
            "match.pattern",
            "path variables are not supported yet",
        ));
    }

    let PatternElement::Path { start, chains } = &pattern_part.anonymous.element else {
        return Err(unsupported(
            "match.pattern",
            "parenthesized and quantified path patterns are not supported yet",
        ));
    };

    let mut plan = GraphPlan::default();
    let start_node = compile_node(start, "match.pattern.nodes[0]")?;
    let mut previous_variable = start_node.variable.clone();
    plan.nodes.push(start_node);

    for (index, chain) in chains.iter().enumerate() {
        let node_path = format!("match.pattern.nodes[{}]", index + 1);
        let next_node = compile_node(&chain.node, node_path)?;
        let relationship_path = format!("match.pattern.relationships[{index}]");
        let relationship = compile_relationship(
            &chain.relationship,
            &previous_variable,
            &next_node.variable,
            relationship_path,
        )?;
        previous_variable.clone_from(&next_node.variable);
        plan.nodes.push(next_node);
        plan.relationships.push(relationship);
    }

    Ok(plan)
}

fn compile_node(
    pattern: &CypherNodePattern,
    path: impl Into<String>,
) -> Result<NodePattern, CoreError> {
    let path = path.into();
    if pattern.properties.is_some() {
        return Err(unsupported(
            format!("{path}.properties"),
            "inline node property maps are not supported yet; use WHERE predicates",
        ));
    }

    Ok(NodePattern {
        variable: required_variable(pattern.variable.as_ref(), format!("{path}.variable"))?,
        label: single_static_label(&pattern.labels, format!("{path}.labels"))?,
    })
}

fn compile_relationship(
    pattern: &CypherRelationshipPattern,
    left: &str,
    right: &str,
    path: impl Into<String>,
) -> Result<RelationshipPattern, CoreError> {
    let path = path.into();
    if pattern.quantifier.is_some() {
        return Err(unsupported(
            format!("{path}.quantifier"),
            "quantified relationship patterns are not supported yet",
        ));
    }

    let direction = match pattern.direction {
        CypherRelationshipDirection::Right => Direction::Outgoing,
        CypherRelationshipDirection::Left => Direction::Incoming,
        CypherRelationshipDirection::Both | CypherRelationshipDirection::Undirected => {
            return Err(unsupported(
                format!("{path}.direction"),
                "relationships must be directed with exactly one arrowhead",
            ));
        }
    };

    let detail = pattern.detail.as_ref().ok_or_else(|| {
        unsupported(
            format!("{path}.detail"),
            "relationship type is required for virtual graph queries",
        )
    })?;
    if detail.range.is_some() {
        return Err(unsupported(
            format!("{path}.range"),
            "variable-length relationship ranges are not supported yet",
        ));
    }
    if detail.properties.is_some() {
        return Err(unsupported(
            format!("{path}.properties"),
            "inline relationship property maps are not supported yet; use WHERE predicates",
        ));
    }

    let relationship_type = detail.types.as_ref().ok_or_else(|| {
        unsupported(
            format!("{path}.types"),
            "relationship type is required for virtual graph queries",
        )
    })?;

    Ok(RelationshipPattern {
        variable: detail.variable.as_ref().map(variable_name),
        relationship_type: single_static_label(
            std::slice::from_ref(relationship_type),
            format!("{path}.types"),
        )?,
        left: left.to_string(),
        direction,
        right: right.to_string(),
    })
}

fn compile_return(return_clause: &Return, plan: &mut GraphPlan) -> Result<(), CoreError> {
    if return_clause.distinct {
        return Err(unsupported(
            "return.distinct",
            "RETURN DISTINCT is not supported yet",
        ));
    }
    if return_clause.star {
        return Err(unsupported("return.star", "RETURN * is not supported yet"));
    }
    if return_clause.skip.is_some() {
        return Err(unsupported("return.skip", "SKIP is not supported yet"));
    }
    if return_clause.items.is_empty() {
        return Err(unsupported(
            "return.items",
            "RETURN must include at least one projection",
        ));
    }

    let mut saw_count = false;
    let mut saw_property = false;
    for (index, item) in return_clause.items.iter().enumerate() {
        match compile_projection(item, format!("return.items[{index}]"))? {
            Projection::CountAll { alias } => {
                saw_count = true;
                plan.projections.push(Projection::CountAll { alias });
            }
            projection @ Projection::Property { .. } => {
                saw_property = true;
                plan.projections.push(projection);
            }
        }
    }
    if saw_count && saw_property {
        return Err(unsupported(
            "return.items",
            "COUNT(*) cannot be mixed with property projections until grouping is supported",
        ));
    }

    if let Some(order) = &return_clause.order {
        for (index, item) in order.items.iter().enumerate() {
            plan.order_by.push(OrderKey {
                property: compile_property_ref(
                    &item.expression,
                    format!("return.order.items[{index}].expression"),
                )?,
                direction: match item.direction {
                    Some(SortDirection::Descending) => OrderDirection::Descending,
                    Some(SortDirection::Ascending) | None => OrderDirection::Ascending,
                },
            });
        }
    }

    if let Some(limit) = &return_clause.limit {
        plan.limit = Some(compile_limit(limit, "return.limit")?);
    }

    Ok(())
}

fn compile_projection(
    item: &ProjectionItem,
    path: impl Into<String>,
) -> Result<Projection, CoreError> {
    let path = path.into();
    match &item.expression {
        Expression::CountStar { .. } => Ok(Projection::CountAll {
            alias: item
                .alias
                .as_ref()
                .map_or_else(|| "count".to_string(), variable_name),
        }),
        expression => Ok(Projection::Property {
            property: compile_property_ref(expression, format!("{path}.expression"))?,
            alias: item.alias.as_ref().map(variable_name),
        }),
    }
}

fn append_predicates(
    expression: &Expression,
    predicates: &mut Vec<PropertyPredicate>,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => append_predicates(inner, predicates, path),
        Expression::BinaryOp {
            op: CypherBinaryOperator::And,
            lhs,
            rhs,
            ..
        } => {
            append_predicates(lhs, predicates, format!("{path}.lhs"))?;
            append_predicates(rhs, predicates, format!("{path}.rhs"))
        }
        Expression::BinaryOp {
            op: CypherBinaryOperator::Or | CypherBinaryOperator::Xor,
            ..
        } => Err(unsupported(
            path,
            "WHERE OR and XOR are not supported yet; use property comparisons joined by AND",
        )),
        Expression::Comparison { lhs, operators, .. } => {
            predicates.push(compile_comparison(lhs, operators.as_slice(), path)?);
            Ok(())
        }
        _ => Err(unsupported(
            path,
            "WHERE only supports property comparisons joined by AND",
        )),
    }
}

fn compile_comparison(
    lhs: &Expression,
    operators: &[(CypherComparisonOperator, Box<Expression>)],
    path: impl Into<String>,
) -> Result<PropertyPredicate, CoreError> {
    let path = path.into();
    let [(operator, rhs)] = operators else {
        return Err(unsupported(
            path,
            "chained comparisons are not supported yet",
        ));
    };

    Ok(PropertyPredicate {
        property: compile_property_ref(lhs, format!("{path}.lhs"))?,
        operator: compile_comparison_operator(*operator, format!("{path}.operator"))?,
        literal: compile_literal(rhs, format!("{path}.rhs"))?,
    })
}

fn compile_property_ref(
    expression: &Expression,
    path: impl Into<String>,
) -> Result<PropertyRef, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_property_ref(inner, path),
        Expression::PropertyLookup { base, property, .. } => {
            let Expression::Variable(variable) = base.as_ref() else {
                return Err(unsupported(
                    format!("{path}.base"),
                    "property references must be variable.property",
                ));
            };
            Ok(PropertyRef {
                variable: variable_name(variable),
                property: property.name.name.clone(),
            })
        }
        _ => Err(unsupported(
            path,
            "only variable.property expressions are supported here",
        )),
    }
}

fn compile_literal(expression: &Expression, path: impl Into<String>) -> Result<Literal, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_literal(inner, path),
        Expression::Literal(CypherLiteral::String(value)) => {
            Ok(Literal::String(value.value.clone()))
        }
        Expression::Literal(CypherLiteral::Number(NumberLiteral::Integer(value))) => {
            Ok(Literal::Integer(*value))
        }
        Expression::Literal(CypherLiteral::Number(NumberLiteral::Float(_))) => Err(unsupported(
            path,
            "floating-point literals are not supported yet",
        )),
        Expression::Literal(CypherLiteral::Boolean(value)) => Ok(Literal::Boolean(*value)),
        Expression::Literal(CypherLiteral::Null) => Ok(Literal::Null),
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            operand,
            ..
        } => match compile_literal(operand, path)? {
            Literal::Integer(value) => Ok(Literal::Integer(-value)),
            _ => Err(unsupported(
                "literal",
                "only integer literals can be negated",
            )),
        },
        Expression::Parameter(_) => Err(unsupported(
            path,
            "parameters are not supported in virtual graph queries yet",
        )),
        _ => Err(unsupported(
            path,
            "only string, integer, boolean, and null literals are supported",
        )),
    }
}

fn compile_limit(expression: &Expression, path: impl Into<String>) -> Result<u64, CoreError> {
    let path = path.into();
    match compile_literal(expression, path)? {
        Literal::Integer(value) => u64::try_from(value).map_err(|conversion_error| {
            unsupported(
                "return.limit",
                format!("LIMIT must be a non-negative integer literal: {conversion_error}"),
            )
        }),
        _ => Err(unsupported(
            "return.limit",
            "LIMIT must be a non-negative integer literal",
        )),
    }
}

fn compile_comparison_operator(
    operator: CypherComparisonOperator,
    path: impl Into<String>,
) -> Result<ComparisonOperator, CoreError> {
    match operator {
        CypherComparisonOperator::Eq => Ok(ComparisonOperator::Equal),
        CypherComparisonOperator::Ne => Ok(ComparisonOperator::NotEqual),
        CypherComparisonOperator::Gt => Ok(ComparisonOperator::GreaterThan),
        CypherComparisonOperator::Ge => Ok(ComparisonOperator::GreaterThanOrEqual),
        CypherComparisonOperator::Lt => Ok(ComparisonOperator::LessThan),
        CypherComparisonOperator::Le => Ok(ComparisonOperator::LessThanOrEqual),
        CypherComparisonOperator::RegexMatch
        | CypherComparisonOperator::StartsWith
        | CypherComparisonOperator::EndsWith
        | CypherComparisonOperator::Contains => Err(unsupported(
            path,
            "string and regex comparison operators are not supported yet",
        )),
    }
}

fn required_variable(
    variable: Option<&Variable>,
    path: impl Into<String>,
) -> Result<String, CoreError> {
    variable
        .map(variable_name)
        .ok_or_else(|| unsupported(path, "node variables are required"))
}

fn single_static_label(
    labels: &[LabelExpression],
    path: impl Into<String>,
) -> Result<String, CoreError> {
    let path = path.into();
    let [label] = labels else {
        return Err(unsupported(
            path,
            "exactly one static label or relationship type is required",
        ));
    };
    let LabelExpression::Static(name) = label else {
        return Err(unsupported(
            path,
            "dynamic and compound label expressions are not supported yet",
        ));
    };
    Ok(name.name.clone())
}

fn variable_name(variable: &Variable) -> String {
    variable.name.name.clone()
}

fn unsupported(path: impl Into<String>, message: impl Into<String>) -> CoreError {
    Diagnostic::new("UNSUPPORTED_CYPHER", path, message).into_core_error()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiles_match_where_return_order_limit() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WHERE service.tier = 'prod' AND person.active = true \
             RETURN person.name AS owner, service.name AS service \
             ORDER BY service.name DESC LIMIT 10",
        )
        .expect("query should compile");

        assert_eq!(
            plan.nodes,
            vec![
                NodePattern {
                    variable: "person".to_string(),
                    label: "Person".to_string(),
                },
                NodePattern {
                    variable: "service".to_string(),
                    label: "Service".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.relationships,
            vec![RelationshipPattern {
                variable: None,
                relationship_type: "OWNS".to_string(),
                left: "person".to_string(),
                direction: Direction::Outgoing,
                right: "service".to_string(),
            }]
        );
        assert_eq!(plan.projections.len(), 2);
        assert_eq!(plan.predicates.len(), 2);
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                direction: OrderDirection::Descending,
            }]
        );
        assert_eq!(plan.limit, Some(10));
    }

    #[test]
    fn compiles_reverse_relationship_direction() {
        let plan = compile_cypher(
            "MATCH (service:Service)<-[ownership:OWNS]-(person:Person) \
             RETURN ownership.source AS source",
        )
        .expect("query should compile");

        assert_eq!(
            plan.relationships,
            vec![RelationshipPattern {
                variable: Some("ownership".to_string()),
                relationship_type: "OWNS".to_string(),
                left: "service".to_string(),
                direction: Direction::Incoming,
                right: "person".to_string(),
            }]
        );
        assert_eq!(
            plan.projections,
            vec![Projection::Property {
                property: PropertyRef {
                    variable: "ownership".to_string(),
                    property: "source".to_string(),
                },
                alias: Some("source".to_string()),
            }]
        );
    }

    #[test]
    fn compiles_count_star_projection() {
        let plan = compile_cypher("MATCH (service:Service) RETURN count(*) AS services")
            .expect("query should compile");

        assert_eq!(
            plan.projections,
            vec![Projection::CountAll {
                alias: "services".to_string(),
            }]
        );
    }

    #[test]
    fn rejects_optional_match() {
        assert_unsupported("OPTIONAL MATCH (service:Service) RETURN service.name");
    }

    #[test]
    fn rejects_write_queries() {
        assert_unsupported("CREATE (service:Service) RETURN service");
    }

    #[test]
    fn rejects_variable_length_relationships() {
        assert_unsupported("MATCH (a:Service)-[:DEPENDS_ON*1..3]->(b:Service) RETURN a.name");
    }

    #[test]
    fn rejects_undirected_relationships() {
        assert_unsupported("MATCH (a:Service)-[:DEPENDS_ON]-(b:Service) RETURN a.name");
    }

    #[test]
    fn rejects_mixed_count_and_property_projection() {
        assert_unsupported("MATCH (service:Service) RETURN service.name, count(*) AS services");
    }

    fn assert_unsupported(cypher: &str) {
        let error = compile_cypher(cypher).expect_err("query should be rejected");
        assert!(
            error.to_string().contains("UNSUPPORTED_CYPHER"),
            "unexpected error: {error}"
        );
    }
}
