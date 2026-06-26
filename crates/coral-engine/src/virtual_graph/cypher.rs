use decypher::ast::clause::{Match, ProjectionItem, Return, SortDirection};
use decypher::ast::expr::{
    BinaryOperator as CypherBinaryOperator, ComparisonOperator as CypherComparisonOperator,
    Expression, Literal as CypherLiteral, NumberLiteral, UnaryOperator,
};
use decypher::ast::names::Variable;
use decypher::ast::pattern::{
    LabelExpression, NodePattern as CypherNodePattern, PatternElement, Properties,
    RelationshipDirection as CypherRelationshipDirection,
    RelationshipPattern as CypherRelationshipPattern,
};
use decypher::ast::query::{
    Query, QueryBody, ReadingClause, SinglePartBody, SinglePartQuery, SingleQueryKind,
};

use super::diagnostic::Diagnostic;
use super::ir::{
    ComparisonOperator, Direction, GraphPlan, Literal, NodePattern, OrderDirection, OrderKey,
    PredicateExpression, PredicateRhs, Projection, PropertyPredicate, PropertyRef,
    RelationshipPattern,
};
use crate::CoreError;

#[derive(Debug)]
struct CompiledNode {
    variable: String,
    pattern: Option<NodePattern>,
    predicates: Vec<PropertyPredicate>,
}

#[derive(Debug)]
struct CompiledRelationship {
    pattern: RelationshipPattern,
    predicates: Vec<PropertyPredicate>,
}

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
        let predicate = compile_predicate_expression(where_clause, "where")?;
        append_predicate_expression(predicate, &mut plan);
    }
    compile_return(return_clause, &mut plan)?;
    Ok(plan)
}

fn compile_match(match_clause: &Match) -> Result<GraphPlan, CoreError> {
    if match_clause.optional {
        return Err(unsupported("match", "OPTIONAL MATCH is not supported yet"));
    }

    let mut plan = GraphPlan::default();

    if match_clause.pattern.parts.is_empty() {
        return Err(unsupported(
            "match.pattern",
            "MATCH pattern must not be empty",
        ));
    }

    for (part_index, pattern_part) in match_clause.pattern.parts.iter().enumerate() {
        if pattern_part.variable.is_some() {
            return Err(unsupported(
                format!("match.pattern.parts[{part_index}]"),
                "path variables are not supported yet",
            ));
        }

        let PatternElement::Path { start, chains } = &pattern_part.anonymous.element else {
            return Err(unsupported(
                format!("match.pattern.parts[{part_index}]"),
                "parenthesized and quantified path patterns are not supported yet",
            ));
        };

        let start_node = compile_node(
            start,
            &plan,
            format!("match.pattern.parts[{part_index}].nodes[0]"),
        )?;
        let mut previous_variable = start_node.variable.clone();
        plan.predicates.extend(start_node.predicates);
        if let Some(pattern) = start_node.pattern {
            plan.nodes.push(pattern);
        }

        for (chain_index, chain) in chains.iter().enumerate() {
            let node_path = format!(
                "match.pattern.parts[{part_index}].nodes[{}]",
                chain_index + 1
            );
            let next_node = compile_node(&chain.node, &plan, node_path)?;
            let next_variable = next_node.variable.clone();
            let relationship_index = plan.relationships.len();
            let relationship_path =
                format!("match.pattern.parts[{part_index}].relationships[{chain_index}]");
            let relationship = compile_relationship(
                &chain.relationship,
                &previous_variable,
                &next_variable,
                relationship_index,
                &plan,
                relationship_path,
            )?;
            previous_variable = next_variable;
            plan.predicates.extend(next_node.predicates);
            if let Some(pattern) = next_node.pattern {
                plan.nodes.push(pattern);
            }
            plan.predicates.extend(relationship.predicates);
            plan.relationships.push(relationship.pattern);
        }
    }

    Ok(plan)
}

fn compile_node(
    pattern: &CypherNodePattern,
    plan: &GraphPlan,
    path: impl Into<String>,
) -> Result<CompiledNode, CoreError> {
    let path = path.into();
    let variable = required_variable(pattern.variable.as_ref(), format!("{path}.variable"))?;
    let label = optional_single_static_label(&pattern.labels, format!("{path}.labels"))?;
    let predicates = pattern.properties.as_ref().map_or_else(
        || Ok(Vec::new()),
        |properties| compile_inline_properties(properties, &variable, format!("{path}.properties")),
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
        pattern: Some(NodePattern { variable, label }),
        predicates,
    })
}

fn compile_inline_properties(
    properties: &Properties,
    variable: &str,
    path: impl Into<String>,
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
            rhs: PredicateRhs::Literal(compile_literal(
                expression,
                format!("{path}.entries[{index}].value"),
            )?),
        });
    }
    Ok(predicates)
}

fn compile_relationship(
    pattern: &CypherRelationshipPattern,
    left: &str,
    right: &str,
    index: usize,
    plan: &GraphPlan,
    path: impl Into<String>,
) -> Result<CompiledRelationship, CoreError> {
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
    let relationship_type = detail.types.as_ref().ok_or_else(|| {
        unsupported(
            format!("{path}.types"),
            "relationship type is required for virtual graph queries",
        )
    })?;
    let variable = detail
        .variable
        .as_ref()
        .map(validate_variable)
        .transpose()?
        .or_else(|| {
            detail
                .properties
                .as_ref()
                .map(|_| fresh_internal_relationship_variable(plan, right, index))
        });
    let predicates = match (&detail.properties, &variable) {
        (Some(properties), Some(variable)) => {
            compile_inline_properties(properties, variable, format!("{path}.properties"))?
        }
        (Some(_), None) => {
            return Err(CoreError::internal(
                "relationship property predicates require a relationship variable",
            ));
        }
        (None, _) => Vec::new(),
    };

    Ok(CompiledRelationship {
        pattern: RelationshipPattern {
            variable,
            relationship_type: single_static_label(
                std::slice::from_ref(relationship_type),
                format!("{path}.types"),
            )?,
            left: left.to_string(),
            direction,
            right: right.to_string(),
        },
        predicates,
    })
}

fn compile_return(return_clause: &Return, plan: &mut GraphPlan) -> Result<(), CoreError> {
    plan.distinct = return_clause.distinct;
    if return_clause.star {
        return Err(unsupported("return.star", "RETURN * is not supported yet"));
    }
    if let Some(skip) = &return_clause.skip {
        plan.skip = Some(compile_skip(skip, "return.skip")?);
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
                property: compile_order_property(
                    &item.expression,
                    &plan.projections,
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

fn compile_order_property(
    expression: &Expression,
    projections: &[Projection],
    path: impl Into<String>,
) -> Result<PropertyRef, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_order_property(inner, projections, path),
        Expression::Variable(variable) => {
            projection_property_for_alias(variable, projections, path)
        }
        _ => compile_property_ref(expression, path),
    }
}

fn projection_property_for_alias(
    variable: &Variable,
    projections: &[Projection],
    path: impl Into<String>,
) -> Result<PropertyRef, CoreError> {
    let path = path.into();
    let alias = variable_name(variable);
    let mut found_property = None;
    for projection in projections {
        match projection {
            Projection::Property {
                property,
                alias: Some(projection_alias),
            } if projection_alias == &alias => {
                if found_property.is_some() {
                    return Err(unsupported(
                        path,
                        format!("ORDER BY alias '{alias}' is ambiguous"),
                    ));
                }
                found_property = Some(property.clone());
            }
            Projection::CountAll {
                alias: projection_alias,
            } if projection_alias == &alias => {
                return Err(unsupported(
                    path,
                    "ORDER BY aggregate aliases is not supported until grouping is supported",
                ));
            }
            _ => {}
        }
    }
    found_property.ok_or_else(|| {
        unsupported(
            path,
            format!("ORDER BY alias '{alias}' does not match a property projection"),
        )
    })
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

fn compile_predicate_expression(
    expression: &Expression,
    path: impl Into<String>,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_predicate_expression(inner, path),
        Expression::BinaryOp {
            op: CypherBinaryOperator::And,
            lhs,
            rhs,
            ..
        } => Ok(PredicateExpression::And {
            left: Box::new(compile_predicate_expression(lhs, format!("{path}.lhs"))?),
            right: Box::new(compile_predicate_expression(rhs, format!("{path}.rhs"))?),
        }),
        Expression::BinaryOp {
            op: CypherBinaryOperator::Or,
            lhs,
            rhs,
            ..
        } => Ok(PredicateExpression::Or {
            left: Box::new(compile_predicate_expression(lhs, format!("{path}.lhs"))?),
            right: Box::new(compile_predicate_expression(rhs, format!("{path}.rhs"))?),
        }),
        Expression::BinaryOp {
            op: CypherBinaryOperator::Xor,
            ..
        } => Err(unsupported(path, "WHERE XOR is not supported yet")),
        Expression::UnaryOp {
            op: UnaryOperator::Not,
            operand,
            ..
        } => Ok(PredicateExpression::Not {
            expression: Box::new(compile_predicate_expression(
                operand,
                format!("{path}.operand"),
            )?),
        }),
        Expression::Comparison { lhs, operators, .. } => Ok(PredicateExpression::Comparison(
            compile_comparison(lhs, operators.as_slice(), path)?,
        )),
        Expression::IsNull {
            operand, negated, ..
        } => Ok(PredicateExpression::Comparison(PropertyPredicate {
            property: compile_property_ref(operand, format!("{path}.operand"))?,
            operator: if *negated {
                ComparisonOperator::NotEqual
            } else {
                ComparisonOperator::Equal
            },
            rhs: PredicateRhs::Literal(Literal::Null),
        })),
        _ => Err(unsupported(
            path,
            "WHERE only supports property comparisons combined with AND, OR, and NOT",
        )),
    }
}

fn append_predicate_expression(expression: PredicateExpression, plan: &mut GraphPlan) {
    if is_conjunctive_expression(&expression) {
        append_conjunctive_expression(expression, &mut plan.predicates);
    } else {
        plan.predicate = Some(match plan.predicate.take() {
            Some(existing) => PredicateExpression::And {
                left: Box::new(existing),
                right: Box::new(expression),
            },
            None => expression,
        });
    }
}

fn is_conjunctive_expression(expression: &PredicateExpression) -> bool {
    match expression {
        PredicateExpression::Comparison(_) => true,
        PredicateExpression::And { left, right } => {
            is_conjunctive_expression(left) && is_conjunctive_expression(right)
        }
        PredicateExpression::Or { .. } | PredicateExpression::Not { .. } => false,
    }
}

fn append_conjunctive_expression(
    expression: PredicateExpression,
    predicates: &mut Vec<PropertyPredicate>,
) {
    match expression {
        PredicateExpression::Comparison(predicate) => predicates.push(predicate),
        PredicateExpression::And { left, right } => {
            append_conjunctive_expression(*left, predicates);
            append_conjunctive_expression(*right, predicates);
        }
        PredicateExpression::Or { .. } | PredicateExpression::Not { .. } => {
            unreachable!("non-conjunctive predicate expression reached conjunctive appender")
        }
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
        rhs: compile_predicate_rhs(rhs, format!("{path}.rhs"))?,
    })
}

fn compile_predicate_rhs(
    expression: &Expression,
    path: impl Into<String>,
) -> Result<PredicateRhs, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_predicate_rhs(inner, path),
        Expression::PropertyLookup { .. } => Ok(PredicateRhs::Property(compile_property_ref(
            expression, path,
        )?)),
        _ => Ok(PredicateRhs::Literal(compile_literal(expression, path)?)),
    }
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
    compile_non_negative_integer(expression, path, "LIMIT")
}

fn compile_skip(expression: &Expression, path: impl Into<String>) -> Result<u64, CoreError> {
    compile_non_negative_integer(expression, path, "SKIP")
}

fn compile_non_negative_integer(
    expression: &Expression,
    path: impl Into<String>,
    keyword: &str,
) -> Result<u64, CoreError> {
    let path = path.into();
    match compile_literal(expression, path.clone())? {
        Literal::Integer(value) => u64::try_from(value).map_err(|conversion_error| {
            unsupported(
                path.clone(),
                format!("{keyword} must be a non-negative integer literal: {conversion_error}"),
            )
        }),
        _ => Err(unsupported(
            path,
            format!("{keyword} must be a non-negative integer literal"),
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
        .map(validate_variable)
        .transpose()?
        .ok_or_else(|| unsupported(path, "node variables are required"))
}

fn validate_variable(variable: &Variable) -> Result<String, CoreError> {
    let name = variable_name(variable);
    if name.starts_with("__coral_") {
        return Err(unsupported(
            "variable",
            "variables beginning with __coral_ are reserved for virtual graph planning",
        ));
    }
    Ok(name)
}

fn fresh_internal_relationship_variable(
    plan: &GraphPlan,
    next_node_variable: &str,
    index: usize,
) -> String {
    let mut suffix = 0;
    loop {
        let candidate = if suffix == 0 {
            format!("__coral_rel_{index}")
        } else {
            format!("__coral_rel_{index}_{suffix}")
        };
        if !plan_uses_variable(plan, &candidate) && next_node_variable != candidate {
            return candidate;
        }
        suffix += 1;
    }
}

fn plan_uses_variable(plan: &GraphPlan, candidate: &str) -> bool {
    plan.nodes.iter().any(|node| node.variable == candidate)
        || plan
            .relationships
            .iter()
            .any(|relationship| relationship.variable.as_deref() == Some(candidate))
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

fn optional_single_static_label(
    labels: &[LabelExpression],
    path: impl Into<String>,
) -> Result<Option<String>, CoreError> {
    if labels.is_empty() {
        return Ok(None);
    }
    single_static_label(labels, path).map(Some)
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
        assert_eq!(plan.predicate, None);
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
    fn compiles_connected_comma_separated_patterns_with_reused_nodes() {
        let plan = compile_cypher(
            "MATCH (source:Service)-[:DEPENDS_ON]->(middle:Service), \
                   (middle)-[:DEPENDS_ON]->(target:Service), \
                   (source)-[:DEPENDS_ON]->(target) \
             RETURN source.name AS source, middle.name AS middle, target.name AS target",
        )
        .expect("query should compile");

        assert_eq!(
            plan.nodes,
            vec![
                NodePattern {
                    variable: "source".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "middle".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "target".to_string(),
                    label: "Service".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.relationships,
            vec![
                RelationshipPattern {
                    variable: None,
                    relationship_type: "DEPENDS_ON".to_string(),
                    left: "source".to_string(),
                    direction: Direction::Outgoing,
                    right: "middle".to_string(),
                },
                RelationshipPattern {
                    variable: None,
                    relationship_type: "DEPENDS_ON".to_string(),
                    left: "middle".to_string(),
                    direction: Direction::Outgoing,
                    right: "target".to_string(),
                },
                RelationshipPattern {
                    variable: None,
                    relationship_type: "DEPENDS_ON".to_string(),
                    left: "source".to_string(),
                    direction: Direction::Outgoing,
                    right: "target".to_string(),
                },
            ]
        );
    }

    #[test]
    fn compiles_repeated_node_property_maps_as_additional_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service {tier: 'prod'}), (service {team: 'platform'}) \
             RETURN service.name",
        )
        .expect("query should compile");

        assert_eq!(
            plan.nodes,
            vec![NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            }]
        );
        assert_eq!(
            plan.predicates,
            vec![
                PropertyPredicate {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
                },
                PropertyPredicate {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "team".to_string(),
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: PredicateRhs::Literal(Literal::String("platform".to_string())),
                },
            ]
        );
    }

    #[test]
    fn compiles_property_to_property_predicates() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WHERE person.team = service.team \
             RETURN service.name",
        )
        .expect("query should compile");

        assert_eq!(
            plan.predicates,
            vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "team".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "team".to_string(),
                }),
            }]
        );
    }

    #[test]
    fn compiles_or_predicates_as_boolean_expression_tree() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE service.tier = 'prod' OR service.tier IS NULL \
             RETURN service.name",
        )
        .expect("query should compile");

        assert!(plan.predicates.is_empty());
        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::Or {
                left: Box::new(PredicateExpression::Comparison(PropertyPredicate {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
                })),
                right: Box::new(PredicateExpression::Comparison(PropertyPredicate {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: PredicateRhs::Literal(Literal::Null),
                })),
            })
        );
    }

    #[test]
    fn compiles_not_predicates_as_boolean_expression_tree() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE NOT (service.tier = 'prod') \
             RETURN service.name",
        )
        .expect("query should compile");

        assert!(plan.predicates.is_empty());
        assert!(matches!(
            plan.predicate,
            Some(PredicateExpression::Not { .. })
        ));
    }

    #[test]
    fn preserves_parenthesized_boolean_precedence() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE service.team = 'platform' AND (service.tier = 'prod' OR service.tier IS NULL) \
             RETURN service.name",
        )
        .expect("query should compile");

        assert!(plan.predicates.is_empty());
        assert!(matches!(
            plan.predicate,
            Some(PredicateExpression::And { .. })
        ));
    }

    #[test]
    fn combines_inline_property_maps_with_boolean_where_tree() {
        let plan = compile_cypher(
            "MATCH (service:Service {team: 'platform'}) \
             WHERE service.tier = 'prod' OR service.tier IS NULL \
             RETURN service.name",
        )
        .expect("query should compile");

        assert_eq!(plan.predicates.len(), 1);
        assert!(matches!(
            plan.predicate,
            Some(PredicateExpression::Or { .. })
        ));
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
    fn compiles_return_distinct() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[:OWNS]->(service:Service) \
             RETURN DISTINCT service.tier AS tier ORDER BY tier",
        )
        .expect("query should compile");

        assert!(plan.distinct);
        assert_eq!(plan.projections.len(), 1);
        assert_eq!(plan.order_by.len(), 1);
    }

    #[test]
    fn compiles_skip_and_limit() {
        let plan = compile_cypher(
            "MATCH (service:Service) RETURN service.name AS service ORDER BY service SKIP 1 LIMIT 2",
        )
        .expect("query should compile");

        assert_eq!(plan.skip, Some(1));
        assert_eq!(plan.limit, Some(2));
    }

    #[test]
    fn rejects_negative_skip() {
        let error = compile_cypher("MATCH (service:Service) RETURN service.name SKIP -1")
            .expect_err("negative SKIP should fail");

        assert!(
            error.to_string().contains("UNSUPPORTED_CYPHER"),
            "{error:?}"
        );
    }

    #[test]
    fn compiles_inline_node_property_maps_as_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service {tier: 'prod', active: true}) RETURN service.name",
        )
        .expect("query should compile");

        assert_eq!(
            plan.predicates,
            vec![
                PropertyPredicate {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
                },
                PropertyPredicate {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "active".to_string(),
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: PredicateRhs::Literal(Literal::Boolean(true)),
                },
            ]
        );
    }

    #[test]
    fn compiles_named_inline_relationship_property_maps_as_predicates() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[ownership:OWNS {source: 'catalog'}]->(service:Service) \
             RETURN service.name",
        )
        .expect("query should compile");

        assert_eq!(
            plan.relationships,
            vec![RelationshipPattern {
                variable: Some("ownership".to_string()),
                relationship_type: "OWNS".to_string(),
                left: "person".to_string(),
                direction: Direction::Outgoing,
                right: "service".to_string(),
            }]
        );
        assert_eq!(
            plan.predicates,
            vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "ownership".to_string(),
                    property: "source".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("catalog".to_string())),
            }]
        );
    }

    #[test]
    fn compiles_anonymous_inline_relationship_property_maps_with_internal_variable() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[:OWNS {source: 'catalog'}]->(service:Service) \
             RETURN service.name",
        )
        .expect("query should compile");
        let relationship = plan
            .relationships
            .first()
            .expect("query should contain a relationship");
        let internal_variable = relationship
            .variable
            .as_ref()
            .expect("anonymous property map relationship should get an internal variable");

        assert!(
            internal_variable.starts_with("__coral_rel_"),
            "{internal_variable}"
        );
        assert_eq!(
            plan.predicates,
            vec![PropertyPredicate {
                property: PropertyRef {
                    variable: internal_variable.clone(),
                    property: "source".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("catalog".to_string())),
            }]
        );
    }

    #[test]
    fn compiles_order_by_property_projection_aliases() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN service.name AS service_name \
             ORDER BY service_name DESC",
        )
        .expect("query should compile");

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
    }

    #[test]
    fn compiles_is_null_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) WHERE service.tier IS NULL RETURN service.name",
        )
        .expect("query should compile");

        assert_eq!(
            plan.predicates,
            vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::Null),
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
    fn rejects_xor_predicates() {
        assert_unsupported(
            "MATCH (service:Service) \
             WHERE service.tier = 'prod' XOR service.tier IS NULL \
             RETURN service.name",
        );
    }

    #[test]
    fn rejects_mixed_count_and_property_projection() {
        assert_unsupported("MATCH (service:Service) RETURN service.name, count(*) AS services");
    }

    #[test]
    fn rejects_reserved_internal_variable_prefix() {
        assert_unsupported("MATCH (__coral_rel_0:Service) RETURN __coral_rel_0.name");
    }

    #[test]
    fn rejects_unlabeled_first_node_binding() {
        assert_unsupported("MATCH (source)-[:DEPENDS_ON]->(target:Service) RETURN target.name");
    }

    #[test]
    fn rejects_conflicting_labels_for_reused_node_variables() {
        assert_unsupported(
            "MATCH (source:Service)-[:DEPENDS_ON]->(target:Service), \
                   (source:Person)-[:OWNS]->(target) \
             RETURN target.name",
        );
    }

    #[test]
    fn rejects_order_by_unknown_aliases() {
        assert_unsupported("MATCH (service:Service) RETURN service.name AS name ORDER BY missing");
    }

    #[test]
    fn rejects_order_by_aggregate_aliases() {
        assert_unsupported("MATCH (service:Service) RETURN count(*) AS services ORDER BY services");
    }

    fn assert_unsupported(cypher: &str) {
        let error = compile_cypher(cypher).expect_err("query should be rejected");
        assert!(
            error.to_string().contains("UNSUPPORTED_CYPHER"),
            "unexpected error: {error}"
        );
    }
}
