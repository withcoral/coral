use std::collections::{BTreeMap, BTreeSet};

use decypher::ast::clause::{Match, ProjectionItem, Return, SortDirection, With};
use decypher::ast::expr::{
    BinaryOperator as CypherBinaryOperator, ComparisonOperator as CypherComparisonOperator,
    Expression, FunctionInvocation, Literal as CypherLiteral, NumberLiteral, UnaryOperator,
};
use decypher::ast::names::Variable;
use decypher::ast::pattern::{
    LabelExpression, NodePattern as CypherNodePattern, PatternElement, Properties,
    RelationshipDirection as CypherRelationshipDirection,
    RelationshipPattern as CypherRelationshipPattern,
};
use decypher::ast::query::{
    MultiPartQuery, MultiPartQueryPart, Query, QueryBody, ReadingClause, SinglePartBody,
    SinglePartQuery, SingleQueryKind,
};
use decypher::cst::{AstNode as _, AstToken as _, Ident};
use decypher::syntax::{SyntaxKind, SyntaxNode};
use ordered_float::OrderedFloat;

use super::diagnostic::Diagnostic;
use super::ir::{
    AggregateFunction, AggregateTarget, ComparisonOperator, Direction, GraphPlan, Literal,
    NodePattern, OrderDirection, OrderExpression, OrderKey, PredicateExpression, PredicateRhs,
    Projection, PropertyPredicate, PropertyRef, RelationshipPattern,
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

#[derive(Debug, Default)]
struct CypherCompileContext {
    count_variable_arguments: BTreeMap<(usize, usize), String>,
}

impl CypherCompileContext {
    fn from_source(cypher: &str) -> Self {
        Self {
            count_variable_arguments: collect_count_variable_arguments(cypher),
        }
    }

    fn count_variable_argument(&self, function: &FunctionInvocation) -> Option<&str> {
        self.count_variable_arguments
            .get(&(function.span.start, function.span.end))
            .map(String::as_str)
    }
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
    let context = CypherCompileContext::from_source(cypher);
    compile_query(&query, &context)
}

fn compile_query(query: &Query, context: &CypherCompileContext) -> Result<GraphPlan, CoreError> {
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
    match &single_query.kind {
        SingleQueryKind::SinglePart(single_part) => compile_single_part(single_part, context),
        SingleQueryKind::MultiPart(multi_part) => compile_multi_part(multi_part, context),
    }
}

fn compile_single_part(
    query: &SinglePartQuery,
    context: &CypherCompileContext,
) -> Result<GraphPlan, CoreError> {
    let return_clause = return_clause_from_single_part(query, "query")?;

    let mut plan = GraphPlan::default();
    compile_reading_clauses_into(&query.reading_clauses, "match", &mut plan)?;
    compile_return(return_clause, &mut plan, context)?;
    Ok(plan)
}

fn compile_multi_part(
    query: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<GraphPlan, CoreError> {
    if let Some(plan) = compile_terminal_with_projection(query, context)? {
        return Ok(plan);
    }
    compile_transparent_multi_part(query, context)
}

fn compile_terminal_with_projection(
    query: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<GraphPlan>, CoreError> {
    let terminal_projection_candidate = query
        .parts
        .iter()
        .any(|part| with_requires_terminal_projection(&part.with));
    if !terminal_projection_candidate {
        return Ok(None);
    }
    let [part] = query.parts.as_slice() else {
        return Err(unsupported(
            "query.parts",
            "terminal WITH projections currently support exactly one MATCH ... WITH ... RETURN query part",
        ));
    };
    if !part.updating_clauses.is_empty() {
        return Err(unsupported(
            "parts[0].updating_clauses",
            "write clauses are not supported by Coral virtual graphs",
        ));
    }
    if !query.final_part.reading_clauses.is_empty() {
        return Err(unsupported(
            "final_part.reading_clauses",
            "WITH projection boundaries before another MATCH require staged query planning and are not supported yet",
        ));
    }

    let return_clause = return_clause_from_single_part(&query.final_part, "final_part")?;
    let mut plan = GraphPlan::default();
    compile_reading_clauses_into(&part.reading_clauses, "parts[0].match", &mut plan)?;

    compile_terminal_with_clause(&part.with, &mut plan, context)?;
    validate_terminal_return_aliases(return_clause, &plan.projections)?;
    apply_terminal_return_modifiers(return_clause, &mut plan)?;
    Ok(Some(plan))
}

fn with_requires_terminal_projection(with: &With) -> bool {
    with.items
        .iter()
        .any(|item| item.alias.is_some() || !matches!(&item.expression, Expression::Variable(_)))
}

fn compile_terminal_with_clause(
    with: &With,
    plan: &mut GraphPlan,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    plan.distinct = with.distinct;
    if with.star {
        return Err(unsupported(
            "with.star",
            "WITH * requires scoped query planning and is not supported yet",
        ));
    }
    if with.where_clause.is_some() {
        return Err(unsupported(
            "with.where",
            "WITH WHERE requires staged query planning and is not supported yet",
        ));
    }
    if with.items.is_empty() {
        return Err(unsupported(
            "with.items",
            "WITH must include at least one projection",
        ));
    }

    for (index, item) in with.items.iter().enumerate() {
        if item.alias.is_none() {
            return Err(unsupported(
                format!("with.items[{index}].alias"),
                "terminal WITH projections require explicit aliases",
            ));
        }
        if matches!(&item.expression, Expression::Variable(_)) {
            return Err(unsupported(
                format!("with.items[{index}].expression"),
                "terminal WITH projections support graph properties and aggregates, not graph variable aliases",
            ));
        }
        plan.projections.push(compile_projection(
            item,
            format!("with.items[{index}]"),
            context,
        )?);
    }

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
            });
        }
    }
    if let Some(skip) = &with.skip {
        plan.skip = Some(compile_skip(skip, "with.skip")?);
    }
    if let Some(limit) = &with.limit {
        plan.limit = Some(compile_limit(limit, "with.limit")?);
    }
    Ok(())
}

fn validate_terminal_return_aliases(
    return_clause: &Return,
    projections: &[Projection],
) -> Result<(), CoreError> {
    if return_clause.star {
        return Err(unsupported(
            "final_part.return.star",
            "RETURN * after WITH requires scoped query planning and is not supported yet",
        ));
    }
    if return_clause.items.len() != projections.len() {
        return Err(unsupported(
            "final_part.return.items",
            "terminal RETURN after WITH must pass through every WITH alias in the same order",
        ));
    }
    for (index, (item, projection)) in return_clause
        .items
        .iter()
        .zip(projections.iter())
        .enumerate()
    {
        if item.alias.is_some() {
            return Err(unsupported(
                format!("final_part.return.items[{index}].alias"),
                "terminal RETURN alias renaming after WITH is not supported yet",
            ));
        }
        let Expression::Variable(variable) = &item.expression else {
            return Err(unsupported(
                format!("final_part.return.items[{index}].expression"),
                "terminal RETURN after WITH must project WITH aliases",
            ));
        };
        let alias = variable_name(variable);
        let expected = projection_output_alias(projection).ok_or_else(|| {
            unsupported(
                format!("final_part.return.items[{index}]"),
                "terminal WITH projections require aliases",
            )
        })?;
        if alias != expected {
            return Err(unsupported(
                format!("final_part.return.items[{index}].expression"),
                format!("terminal RETURN expected WITH alias '{expected}', got '{alias}'"),
            ));
        }
    }
    Ok(())
}

fn apply_terminal_return_modifiers(
    return_clause: &Return,
    plan: &mut GraphPlan,
) -> Result<(), CoreError> {
    plan.distinct |= return_clause.distinct;
    if (return_clause.order.is_some()
        || return_clause.skip.is_some()
        || return_clause.limit.is_some())
        && (!plan.order_by.is_empty() || plan.skip.is_some() || plan.limit.is_some())
    {
        return Err(unsupported(
            "final_part.return",
            "terminal WITH and RETURN cannot both define ORDER BY, SKIP, or LIMIT without staged query planning",
        ));
    }
    if let Some(skip) = &return_clause.skip {
        plan.skip = Some(compile_skip(skip, "final_part.return.skip")?);
    }
    if let Some(order) = &return_clause.order {
        for (index, item) in order.items.iter().enumerate() {
            plan.order_by.push(OrderKey {
                expression: compile_terminal_alias_order_expression(
                    &item.expression,
                    &plan.projections,
                    format!("final_part.return.order.items[{index}].expression"),
                )?,
                direction: match item.direction {
                    Some(SortDirection::Descending) => OrderDirection::Descending,
                    Some(SortDirection::Ascending) | None => OrderDirection::Ascending,
                },
            });
        }
    }
    if let Some(limit) = &return_clause.limit {
        plan.limit = Some(compile_limit(limit, "final_part.return.limit")?);
    }
    Ok(())
}

fn compile_terminal_alias_order_expression(
    expression: &Expression,
    projections: &[Projection],
    path: impl Into<String>,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_terminal_alias_order_expression(inner, projections, path)
        }
        Expression::Variable(variable) => {
            projection_order_expression_for_alias(variable, projections, path)
        }
        _ => Err(unsupported(
            path,
            "ORDER BY after terminal WITH only supports projected aliases",
        )),
    }
}

fn projection_output_alias(projection: &Projection) -> Option<&str> {
    match projection {
        Projection::Property { alias, .. } => alias.as_deref(),
        Projection::CountAll { alias } | Projection::Aggregate { alias, .. } => Some(alias),
    }
}

fn compile_transparent_multi_part(
    query: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<GraphPlan, CoreError> {
    let mut plan = GraphPlan::default();
    for (index, part) in query.parts.iter().enumerate() {
        compile_transparent_multi_part_part(part, index, &mut plan)?;
    }

    match query.final_part.reading_clauses.as_slice() {
        [] => {}
        clauses => compile_reading_clauses_into(clauses, "final_part.match", &mut plan)?,
    }
    let return_clause = return_clause_from_single_part(&query.final_part, "final_part")?;
    compile_return(return_clause, &mut plan, context)?;
    Ok(plan)
}

fn compile_transparent_multi_part_part(
    part: &MultiPartQueryPart,
    index: usize,
    plan: &mut GraphPlan,
) -> Result<(), CoreError> {
    if !part.updating_clauses.is_empty() {
        return Err(unsupported(
            format!("parts[{index}].updating_clauses"),
            "write clauses are not supported by Coral virtual graphs",
        ));
    }
    compile_reading_clauses_into(&part.reading_clauses, format!("parts[{index}].match"), plan)?;
    validate_transparent_with(&part.with, plan, format!("parts[{index}].with"))
}

fn validate_transparent_with(
    with: &With,
    plan: &GraphPlan,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    if with.distinct {
        return Err(unsupported(
            format!("{path}.distinct"),
            "WITH DISTINCT requires staged query planning and is not supported yet",
        ));
    }
    if with.star {
        return Err(unsupported(
            format!("{path}.star"),
            "WITH * requires scoped query planning and is not supported yet",
        ));
    }
    if with.order.is_some() || with.skip.is_some() || with.limit.is_some() {
        return Err(unsupported(
            path.clone(),
            "WITH ORDER BY, SKIP, and LIMIT require staged query planning and are not supported yet",
        ));
    }
    if with.where_clause.is_some() {
        return Err(unsupported(
            format!("{path}.where"),
            "WITH WHERE requires staged query planning and is not supported yet",
        ));
    }
    if with.items.is_empty() {
        return Err(unsupported(
            format!("{path}.items"),
            "WITH must carry every currently bound variable in this transparent subset",
        ));
    }

    let mut carried = BTreeSet::new();
    for (index, item) in with.items.iter().enumerate() {
        if item.alias.is_some() {
            return Err(unsupported(
                format!("{path}.items[{index}].alias"),
                "WITH aliases require scoped query planning and are not supported yet",
            ));
        }
        let Expression::Variable(variable) = &item.expression else {
            return Err(unsupported(
                format!("{path}.items[{index}].expression"),
                "transparent WITH only supports pass-through graph variables",
            ));
        };
        carried.insert(variable_name(variable));
    }

    let bound = bound_graph_variables(plan);
    if carried != bound {
        return Err(unsupported(
            format!("{path}.items"),
            "transparent WITH must carry every currently bound graph variable without dropping or adding variables",
        ));
    }

    Ok(())
}

fn bound_graph_variables(plan: &GraphPlan) -> BTreeSet<String> {
    plan.nodes
        .iter()
        .map(|node| node.variable.clone())
        .chain(
            plan.relationships
                .iter()
                .filter_map(|relationship| relationship.variable.clone()),
        )
        .collect()
}

fn compile_reading_clauses_into(
    reading_clauses: &[ReadingClause],
    path: impl Into<String>,
    plan: &mut GraphPlan,
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
                compile_match_into(match_clause, plan)?;
                if let Some(where_clause) = &match_clause.where_clause {
                    let predicate = compile_predicate_expression(
                        where_clause,
                        format!("{path}[{index}].where"),
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

fn return_clause_from_single_part(
    query: &SinglePartQuery,
    path: impl Into<String>,
) -> Result<&Return, CoreError> {
    let path = path.into();
    match &query.body {
        SinglePartBody::Return(return_clause) => Ok(return_clause),
        SinglePartBody::Updating { .. } => Err(unsupported(
            path,
            "write clauses are not supported by Coral virtual graphs",
        )),
        SinglePartBody::Finish(_) => Err(unsupported(
            path,
            "FINISH is not supported because virtual graph queries must return rows",
        )),
    }
}

fn compile_match_into(match_clause: &Match, plan: &mut GraphPlan) -> Result<(), CoreError> {
    if match_clause.optional {
        return Err(unsupported("match", "OPTIONAL MATCH is not supported yet"));
    }

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
            plan,
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
            let next_node = compile_node(&chain.node, plan, node_path)?;
            let next_variable = next_node.variable.clone();
            let relationship_index = plan.relationships.len();
            let relationship_path =
                format!("match.pattern.parts[{part_index}].relationships[{chain_index}]");
            let relationship = compile_relationship(
                &chain.relationship,
                &previous_variable,
                &next_variable,
                relationship_index,
                plan,
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

    Ok(())
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
            Direction::Undirected
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

fn compile_return(
    return_clause: &Return,
    plan: &mut GraphPlan,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
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

    for (index, item) in return_clause.items.iter().enumerate() {
        plan.projections.push(compile_projection(
            item,
            format!("return.items[{index}]"),
            context,
        )?);
    }

    if let Some(order) = &return_clause.order {
        for (index, item) in order.items.iter().enumerate() {
            plan.order_by.push(OrderKey {
                expression: compile_order_expression(
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

fn compile_order_expression(
    expression: &Expression,
    projections: &[Projection],
    path: impl Into<String>,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_order_expression(inner, projections, path),
        Expression::Variable(variable) => {
            projection_order_expression_for_alias(variable, projections, path)
        }
        _ => Ok(OrderExpression::Property(compile_property_ref(
            expression, path,
        )?)),
    }
}

fn projection_order_expression_for_alias(
    variable: &Variable,
    projections: &[Projection],
    path: impl Into<String>,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    let alias = variable_name(variable);
    let mut found_property = None;
    let mut found_projected_alias = false;
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
            }
            | Projection::Aggregate {
                alias: projection_alias,
                ..
            } if projection_alias == &alias => {
                if found_projected_alias {
                    return Err(unsupported(
                        path,
                        format!("ORDER BY alias '{alias}' is ambiguous"),
                    ));
                }
                found_projected_alias = true;
            }
            _ => {}
        }
    }
    if found_property.is_some() && found_projected_alias {
        return Err(unsupported(
            path,
            format!("ORDER BY alias '{alias}' is ambiguous"),
        ));
    }
    if let Some(property) = found_property {
        return Ok(OrderExpression::Property(property));
    }
    if found_projected_alias {
        return Ok(OrderExpression::ProjectionAlias(alias));
    }
    Err(unsupported(
        path,
        format!("ORDER BY alias '{alias}' does not match a projection"),
    ))
}

fn compile_projection(
    item: &ProjectionItem,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    match &item.expression {
        Expression::CountStar { .. } => Ok(Projection::CountAll {
            alias: item
                .alias
                .as_ref()
                .map_or_else(|| "count".to_string(), variable_name),
        }),
        Expression::FunctionCall(function) if compile_aggregate_function(function).is_some() => {
            compile_aggregate_projection(function, item, path, context)
        }
        Expression::FunctionCall(function) => Err(unsupported(
            format!("{path}.expression"),
            format!(
                "RETURN function '{}' is not supported yet",
                qualified_function_name(function)
            ),
        )),
        expression => Ok(Projection::Property {
            property: compile_property_ref(expression, format!("{path}.expression"))?,
            alias: item.alias.as_ref().map(variable_name),
        }),
    }
}

fn compile_aggregate_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let function_kind = compile_aggregate_function(function).ok_or_else(|| {
        unsupported(
            format!("{path}.expression"),
            format!(
                "RETURN function '{}' is not supported yet",
                qualified_function_name(function)
            ),
        )
    })?;
    let target = compile_function_aggregate_target(function, function_kind, &path, context)?;
    Ok(Projection::Aggregate {
        function: function_kind,
        target,
        distinct: function.distinct,
        alias: item.alias.as_ref().map_or_else(
            || aggregate_function_name(function_kind).to_string(),
            variable_name,
        ),
    })
}

fn compile_function_aggregate_target(
    function: &FunctionInvocation,
    function_kind: AggregateFunction,
    path: &str,
    context: &CypherCompileContext,
) -> Result<AggregateTarget, CoreError> {
    match function.arguments.as_slice() {
        [argument] => compile_aggregate_target(argument, format!("{path}.expression.arguments[0]")),
        [] if function_kind == AggregateFunction::Count => {
            let variable = context.count_variable_argument(function).ok_or_else(|| {
                unsupported(
                    format!("{path}.expression.arguments"),
                    "count() supports exactly one graph property or node variable argument; use count(*) to count rows",
                )
            })?;
            Ok(AggregateTarget::VariableKey {
                variable: variable.to_string(),
            })
        }
        _ => Err(unsupported(
            format!("{path}.expression.arguments"),
            format!(
                "{}() supports exactly one graph property argument",
                aggregate_function_name(function_kind)
            ),
        )),
    }
}

fn collect_count_variable_arguments(cypher: &str) -> BTreeMap<(usize, usize), String> {
    // decypher's high-level AST currently drops variable-only function
    // arguments such as count(n); the lossless CST keeps them by span.
    let parse = decypher::parse_cst(cypher);
    let tree = parse.tree();
    tree.syntax()
        .descendants()
        .filter(|node| node.kind() == SyntaxKind::FUNCTION_INVOCATION)
        .filter_map(|node| count_variable_argument_from_cst(&node))
        .collect()
}

fn count_variable_argument_from_cst(node: &SyntaxNode) -> Option<((usize, usize), String)> {
    if !function_invocation_name_is_count(node) {
        return None;
    }

    let mut variables = node
        .children()
        .filter(|child| child.kind() == SyntaxKind::VARIABLE);
    let variable = variables.next()?;
    if variables.next().is_some() {
        return None;
    }
    let variable = variable_name_from_cst(&variable)?;
    let range = node.text_range();
    Some(((range.start().into(), range.end().into()), variable))
}

fn function_invocation_name_is_count(node: &SyntaxNode) -> bool {
    node.children()
        .find(|child| child.kind() == SyntaxKind::FUNCTION_NAME)
        .and_then(|name| name.first_token())
        .is_some_and(|token| token.text().eq_ignore_ascii_case("count"))
}

fn variable_name_from_cst(node: &SyntaxNode) -> Option<String> {
    node.first_token()
        .and_then(Ident::cast)
        .map(|ident| ident.unescape())
}

fn compile_aggregate_target(
    expression: &Expression,
    path: impl Into<String>,
) -> Result<AggregateTarget, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_aggregate_target(inner, path),
        Expression::Variable(variable) => Ok(AggregateTarget::VariableKey {
            variable: variable_name(variable),
        }),
        _ => Ok(AggregateTarget::Property(compile_property_ref(
            expression, path,
        )?)),
    }
}

fn compile_aggregate_function(function: &FunctionInvocation) -> Option<AggregateFunction> {
    let [name] = function.name.as_slice() else {
        return None;
    };
    if name.name.eq_ignore_ascii_case("count") {
        Some(AggregateFunction::Count)
    } else if name.name.eq_ignore_ascii_case("sum") {
        Some(AggregateFunction::Sum)
    } else if name.name.eq_ignore_ascii_case("avg") {
        Some(AggregateFunction::Avg)
    } else if name.name.eq_ignore_ascii_case("min") {
        Some(AggregateFunction::Min)
    } else if name.name.eq_ignore_ascii_case("max") {
        Some(AggregateFunction::Max)
    } else {
        None
    }
}

fn aggregate_function_name(function: AggregateFunction) -> &'static str {
    match function {
        AggregateFunction::Count => "count",
        AggregateFunction::Sum => "sum",
        AggregateFunction::Avg => "avg",
        AggregateFunction::Min => "min",
        AggregateFunction::Max => "max",
    }
}

fn is_exists_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("exists")
    )
}

fn qualified_function_name(function: &FunctionInvocation) -> String {
    function
        .name
        .iter()
        .map(|part| part.name.as_str())
        .collect::<Vec<_>>()
        .join(".")
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
        Expression::Comparison { lhs, operators, .. } => {
            compile_comparison_expression(lhs, operators.as_slice(), path)
        }
        Expression::In { lhs, rhs, .. } => Ok(PredicateExpression::Comparison(
            compile_in_predicate(lhs, rhs, path)?,
        )),
        Expression::Literal(CypherLiteral::Boolean(value)) => {
            Ok(PredicateExpression::Boolean(*value))
        }
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
        Expression::FunctionCall(function) if is_exists_function(function) => Ok(
            PredicateExpression::Comparison(compile_exists_predicate(function, path)?),
        ),
        Expression::PropertyLookup { .. } => {
            Ok(PredicateExpression::Comparison(PropertyPredicate {
                property: compile_property_ref(expression, path)?,
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::Boolean(true)),
            }))
        }
        _ => Err(unsupported(
            path,
            "WHERE only supports property comparisons combined with AND, OR, and NOT",
        )),
    }
}

fn compile_exists_predicate(
    function: &FunctionInvocation,
    path: impl Into<String>,
) -> Result<PropertyPredicate, CoreError> {
    let path = path.into();
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "exists() supports exactly one variable.property argument",
        ));
    };
    Ok(PropertyPredicate {
        property: compile_property_ref(argument, format!("{path}.arguments[0]"))?,
        operator: ComparisonOperator::NotEqual,
        rhs: PredicateRhs::Literal(Literal::Null),
    })
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
        PredicateExpression::Boolean(_)
        | PredicateExpression::Or { .. }
        | PredicateExpression::Not { .. } => false,
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
        PredicateExpression::Boolean(_)
        | PredicateExpression::Or { .. }
        | PredicateExpression::Not { .. } => {
            unreachable!("non-conjunctive predicate expression reached conjunctive appender")
        }
    }
}

fn compile_comparison_expression(
    lhs: &Expression,
    operators: &[(CypherComparisonOperator, Box<Expression>)],
    path: impl Into<String>,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    if operators.is_empty() {
        return Err(unsupported(path, "comparison must include an operator"));
    }

    let (prefix, mut current_lhs) = compile_comparison_prefix(lhs, format!("{path}.lhs"))?;
    let mut expression = prefix;
    for (index, (operator, rhs)) in operators.iter().enumerate() {
        let predicate = compile_binary_comparison(
            current_lhs,
            *operator,
            rhs,
            format!("{path}.operators[{index}]"),
        )?;
        let next = PredicateExpression::Comparison(predicate);
        expression = Some(append_expression_conjunct(expression, next));
        current_lhs = rhs;
    }

    expression.ok_or_else(|| CoreError::internal("comparison expression was empty"))
}

fn compile_comparison_prefix(
    expression: &Expression,
    path: impl Into<String>,
) -> Result<(Option<PredicateExpression>, &Expression), CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_comparison_prefix(inner, path),
        Expression::Comparison { lhs, operators, .. } => Ok((
            Some(compile_comparison_expression(
                lhs,
                operators.as_slice(),
                path,
            )?),
            terminal_comparison_operand(lhs, operators.as_slice()),
        )),
        _ => Ok((None, expression)),
    }
}

fn terminal_comparison_operand<'a>(
    lhs: &'a Expression,
    operators: &'a [(CypherComparisonOperator, Box<Expression>)],
) -> &'a Expression {
    operators.last().map_or(lhs, |(_, rhs)| rhs.as_ref())
}

fn append_expression_conjunct(
    expression: Option<PredicateExpression>,
    next: PredicateExpression,
) -> PredicateExpression {
    match expression {
        Some(previous) => PredicateExpression::And {
            left: Box::new(previous),
            right: Box::new(next),
        },
        None => next,
    }
}

fn compile_binary_comparison(
    lhs: &Expression,
    operator: CypherComparisonOperator,
    rhs: &Expression,
    path: impl Into<String>,
) -> Result<PropertyPredicate, CoreError> {
    let path = path.into();
    let operator = compile_comparison_operator(operator, format!("{path}.operator"))?;
    if let Some(property) = compile_optional_property_ref(lhs, format!("{path}.lhs"))? {
        return Ok(PropertyPredicate {
            property,
            operator,
            rhs: compile_predicate_rhs(rhs, format!("{path}.rhs"))?,
        });
    }
    if let Some(property) = compile_optional_property_ref(rhs, format!("{path}.rhs"))? {
        return Ok(PropertyPredicate {
            property,
            operator: invert_comparison_operator(operator, format!("{path}.operator"))?,
            rhs: PredicateRhs::Literal(compile_literal(lhs, format!("{path}.lhs"))?),
        });
    }

    Err(unsupported(
        path,
        "comparisons must include at least one variable.property operand",
    ))
}

fn compile_in_predicate(
    lhs: &Expression,
    rhs: &Expression,
    path: impl Into<String>,
) -> Result<PropertyPredicate, CoreError> {
    let path = path.into();
    Ok(PropertyPredicate {
        property: compile_property_ref(lhs, format!("{path}.lhs"))?,
        operator: ComparisonOperator::In,
        rhs: PredicateRhs::List(compile_literal_list(rhs, format!("{path}.rhs"))?),
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

fn compile_optional_property_ref(
    expression: &Expression,
    path: impl Into<String>,
) -> Result<Option<PropertyRef>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_optional_property_ref(inner, path),
        Expression::PropertyLookup { .. } => compile_property_ref(expression, path).map(Some),
        _ => Ok(None),
    }
}

fn compile_literal_list(
    expression: &Expression,
    path: impl Into<String>,
) -> Result<Vec<Literal>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_literal_list(inner, path),
        Expression::Literal(CypherLiteral::List(list)) => list
            .elements
            .iter()
            .enumerate()
            .map(|(index, expression)| compile_literal(expression, format!("{path}[{index}]")))
            .collect(),
        Expression::Parameter(_) => Err(unsupported(
            path,
            "parameters are not supported in virtual graph queries yet",
        )),
        _ => Err(unsupported(
            path,
            "IN predicates require a literal list right-hand side",
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
        Expression::Literal(CypherLiteral::Number(NumberLiteral::Float(value))) => {
            compile_float_literal(*value, path)
        }
        Expression::Literal(CypherLiteral::Boolean(value)) => Ok(Literal::Boolean(*value)),
        Expression::Literal(CypherLiteral::Null) => Ok(Literal::Null),
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            operand,
            ..
        } => match compile_literal(operand, path)? {
            Literal::Integer(value) => Ok(Literal::Integer(-value)),
            Literal::Float(value) => Ok(Literal::Float(OrderedFloat(-value.into_inner()))),
            _ => Err(unsupported(
                "literal",
                "only numeric literals can be negated",
            )),
        },
        Expression::Parameter(_) => Err(unsupported(
            path,
            "parameters are not supported in virtual graph queries yet",
        )),
        _ => Err(unsupported(
            path,
            "only string, numeric, boolean, and null literals are supported",
        )),
    }
}

fn compile_float_literal(value: f64, path: impl Into<String>) -> Result<Literal, CoreError> {
    let path = path.into();
    if value.is_finite() {
        Ok(Literal::Float(OrderedFloat(value)))
    } else {
        Err(unsupported(
            path,
            "non-finite floating-point literals are not supported",
        ))
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
        CypherComparisonOperator::StartsWith => Ok(ComparisonOperator::StartsWith),
        CypherComparisonOperator::EndsWith => Ok(ComparisonOperator::EndsWith),
        CypherComparisonOperator::Contains => Ok(ComparisonOperator::Contains),
        CypherComparisonOperator::RegexMatch => Err(unsupported(
            path,
            "regex comparison operators are not supported yet",
        )),
    }
}

fn invert_comparison_operator(
    operator: ComparisonOperator,
    path: impl Into<String>,
) -> Result<ComparisonOperator, CoreError> {
    match operator {
        ComparisonOperator::Equal => Ok(ComparisonOperator::Equal),
        ComparisonOperator::NotEqual => Ok(ComparisonOperator::NotEqual),
        ComparisonOperator::GreaterThan => Ok(ComparisonOperator::LessThan),
        ComparisonOperator::GreaterThanOrEqual => Ok(ComparisonOperator::LessThanOrEqual),
        ComparisonOperator::LessThan => Ok(ComparisonOperator::GreaterThan),
        ComparisonOperator::LessThanOrEqual => Ok(ComparisonOperator::GreaterThanOrEqual),
        ComparisonOperator::In
        | ComparisonOperator::StartsWith
        | ComparisonOperator::EndsWith
        | ComparisonOperator::Contains => Err(unsupported(
            path,
            "this comparison operator requires a variable.property left-hand side",
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
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Descending,
            }]
        );
        assert_eq!(plan.limit, Some(10));
        assert_eq!(plan.predicate, None);
    }

    #[test]
    fn compiles_transparent_with_pass_through() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WITH service \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS source, target.name AS target \
             ORDER BY source, target",
        )
        .expect("transparent WITH query should compile");

        assert_eq!(
            plan.nodes,
            vec![
                NodePattern {
                    variable: "service".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "target".to_string(),
                    label: "Service".to_string(),
                },
            ]
        );
        assert_eq!(plan.relationships.len(), 1);
        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("source".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "target".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("target".to_string()),
                },
            ]
        );
    }

    #[test]
    fn compiles_transparent_with_before_return() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WITH service \
             RETURN service.name AS service \
             ORDER BY service",
        )
        .expect("transparent WITH before RETURN should compile");

        assert_eq!(plan.nodes.len(), 1);
        assert_eq!(plan.relationships.len(), 0);
        assert_eq!(plan.projections.len(), 1);
    }

    #[test]
    fn compiles_multiple_match_clauses() {
        let plan = compile_cypher(
            "MATCH (person:Person) \
             WHERE person.team = 'platform' \
             MATCH (person)-[:OWNS]->(service:Service) \
             WHERE service.tier = 'prod' \
             RETURN person.name AS owner, service.name AS service",
        )
        .expect("multiple MATCH clauses should compile");

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
        assert_eq!(plan.relationships.len(), 1);
        assert_eq!(plan.predicates.len(), 2);
    }

    #[test]
    fn compiles_terminal_with_projection_aliases() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WITH person.name AS owner, count(service) AS services \
             RETURN owner, services \
             ORDER BY services DESC, owner \
             LIMIT 10",
        )
        .expect("terminal WITH projection query should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "person".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("owner".to_string()),
                },
                Projection::Aggregate {
                    function: AggregateFunction::Count,
                    target: AggregateTarget::VariableKey {
                        variable: "service".to_string(),
                    },
                    distinct: false,
                    alias: "services".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.order_by,
            vec![
                OrderKey {
                    expression: OrderExpression::ProjectionAlias("services".to_string()),
                    direction: OrderDirection::Descending,
                },
                OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "person".to_string(),
                        property: "name".to_string(),
                    }),
                    direction: OrderDirection::Ascending,
                },
            ]
        );
        assert_eq!(plan.limit, Some(10));
    }

    #[test]
    fn compiles_terminal_with_distinct_property_projection() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WITH DISTINCT service.tier AS tier \
             RETURN tier \
             ORDER BY tier",
        )
        .expect("terminal WITH DISTINCT projection query should compile");

        assert!(plan.distinct);
        assert_eq!(
            plan.projections,
            vec![Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("tier".to_string()),
            }]
        );
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
            }]
        );
    }

    #[test]
    fn compiles_terminal_with_order_skip_limit() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WITH service.tier AS tier, count(service) AS services \
             ORDER BY services DESC, tier \
             SKIP 1 \
             LIMIT 5 \
             RETURN tier, services",
        )
        .expect("terminal WITH modifiers should compile");

        assert_eq!(
            plan.order_by,
            vec![
                OrderKey {
                    expression: OrderExpression::ProjectionAlias("services".to_string()),
                    direction: OrderDirection::Descending,
                },
                OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    direction: OrderDirection::Ascending,
                },
            ]
        );
        assert_eq!(plan.skip, Some(1));
        assert_eq!(plan.limit, Some(5));
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
    fn compiles_literal_left_comparisons_by_inverting_operator() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE 'prod' = service.tier AND 10 < service.id \
             RETURN service.name",
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
                        property: "id".to_string(),
                    },
                    operator: ComparisonOperator::GreaterThan,
                    rhs: PredicateRhs::Literal(Literal::Integer(10)),
                },
            ]
        );
    }

    #[test]
    fn compiles_float_literals() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE service.risk >= 0.75 AND -1.5 < service.margin \
             RETURN service.name",
        )
        .expect("query should compile");

        assert_eq!(
            plan.predicates,
            vec![
                PropertyPredicate {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    },
                    operator: ComparisonOperator::GreaterThanOrEqual,
                    rhs: PredicateRhs::Literal(Literal::Float(OrderedFloat(0.75_f64))),
                },
                PropertyPredicate {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "margin".to_string(),
                    },
                    operator: ComparisonOperator::GreaterThan,
                    rhs: PredicateRhs::Literal(Literal::Float(OrderedFloat(-1.5_f64))),
                },
            ]
        );
    }

    #[test]
    fn compiles_chained_comparisons_as_conjunctions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE 10 <= service.id < 30 \
             RETURN service.name",
        )
        .expect("query should compile");

        assert_eq!(
            plan.predicates,
            vec![
                PropertyPredicate {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "id".to_string(),
                    },
                    operator: ComparisonOperator::GreaterThanOrEqual,
                    rhs: PredicateRhs::Literal(Literal::Integer(10)),
                },
                PropertyPredicate {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "id".to_string(),
                    },
                    operator: ComparisonOperator::LessThan,
                    rhs: PredicateRhs::Literal(Literal::Integer(30)),
                },
            ]
        );
    }

    #[test]
    fn compiles_in_predicates_with_literal_lists() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE service.tier IN ['prod', 'dev'] \
             RETURN service.name",
        )
        .expect("query should compile");

        assert_eq!(
            plan.predicates,
            vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::In,
                rhs: PredicateRhs::List(vec![
                    Literal::String("prod".to_string()),
                    Literal::String("dev".to_string()),
                ]),
            }]
        );
    }

    #[test]
    fn compiles_string_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE service.name STARTS WITH 'bill' \
                AND service.name ENDS WITH 'api' \
                AND service.name CONTAINS 'ing' \
             RETURN service.name",
        )
        .expect("query should compile");

        assert_eq!(
            plan.predicates
                .iter()
                .map(|predicate| predicate.operator)
                .collect::<Vec<_>>(),
            vec![
                ComparisonOperator::StartsWith,
                ComparisonOperator::EndsWith,
                ComparisonOperator::Contains,
            ]
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
    fn compiles_bare_boolean_property_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE service.active \
             RETURN service.name",
        )
        .expect("bare boolean property query should compile");

        assert_eq!(
            plan.predicates,
            vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "active".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::Boolean(true)),
            }]
        );

        let negated = compile_cypher(
            "MATCH (service:Service) \
             WHERE NOT service.active \
             RETURN service.name",
        )
        .expect("negated bare boolean property query should compile");
        assert!(matches!(
            negated.predicate,
            Some(PredicateExpression::Not { .. })
        ));
    }

    #[test]
    fn compiles_constant_boolean_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE true \
             RETURN service.name",
        )
        .expect("constant true predicate query should compile");

        assert!(plan.predicates.is_empty());
        assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));

        let combined = compile_cypher(
            "MATCH (service:Service) \
             WHERE service.active OR false \
             RETURN service.name",
        )
        .expect("constant false predicate expression query should compile");
        assert!(matches!(
            combined.predicate,
            Some(PredicateExpression::Or { .. })
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
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
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
    fn compiles_exists_property_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) WHERE exists(service.tier) RETURN service.name",
        )
        .expect("exists property query should compile");

        assert_eq!(
            plan.predicates,
            vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::NotEqual,
                rhs: PredicateRhs::Literal(Literal::Null),
            }]
        );

        let negated = compile_cypher(
            "MATCH (service:Service) WHERE NOT exists(service.tier) RETURN service.name",
        )
        .expect("negated exists property query should compile");
        assert!(matches!(
            negated.predicate,
            Some(PredicateExpression::Not { .. })
        ));
    }

    #[test]
    fn rejects_exists_without_single_property_argument() {
        assert_unsupported("MATCH (service:Service) WHERE exists() RETURN service.name");
        assert_unsupported("MATCH (service:Service) WHERE exists(1) RETURN service.name");
        assert_unsupported("MATCH (service:Service) WHERE exists(service) RETURN service.name");
    }

    #[test]
    fn rejects_optional_match() {
        assert_unsupported("OPTIONAL MATCH (service:Service) RETURN service.name");
    }

    #[test]
    fn rejects_non_transparent_with_boundaries() {
        assert_unsupported("MATCH (service:Service) WITH DISTINCT service RETURN service.name");
        assert_unsupported("MATCH (service:Service) WITH * RETURN service.name");
        assert_unsupported("MATCH (service:Service) WITH service LIMIT 1 RETURN service.name");
        assert_unsupported(
            "MATCH (service:Service) WITH service WHERE service.tier = 'prod' RETURN service.name",
        );
        assert_unsupported(
            "MATCH (person:Person)-[:OWNS]->(service:Service) WITH service RETURN service.name",
        );
    }

    #[test]
    fn rejects_terminal_with_projection_boundaries_requiring_staging() {
        assert_unsupported("MATCH (service:Service) WITH service.name RETURN service.name");
        assert_unsupported("MATCH (service:Service) WITH service AS renamed RETURN renamed");
        assert_unsupported("MATCH (service:Service) WITH service.name AS service RETURN missing");
        assert_unsupported(
            "MATCH (service:Service) WITH service.name AS service RETURN service AS renamed",
        );
        assert_unsupported(
            "MATCH (service:Service) WITH service.name AS service WHERE service = 'billing-api' RETURN service",
        );
        assert_unsupported(
            "MATCH (service:Service) WITH service.name AS service MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN service, target.name",
        );
        assert_unsupported(
            "MATCH (service:Service) WITH service.name AS service RETURN service ORDER BY service.name",
        );
        assert_unsupported(
            "MATCH (service:Service) WITH service.name AS service ORDER BY service RETURN service ORDER BY service",
        );
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
    fn compiles_undirected_relationships() {
        let plan = compile_cypher("MATCH (a:Service)-[:DEPENDS_ON]-(b:Service) RETURN a.name")
            .expect("undirected relationship should compile");

        assert_eq!(
            plan.relationships,
            vec![RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "a".to_string(),
                direction: Direction::Undirected,
                right: "b".to_string(),
            }]
        );
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
    fn rejects_parameterized_in_predicates() {
        assert_unsupported(
            "MATCH (service:Service) WHERE service.tier IN $tiers RETURN service.name",
        );
    }

    #[test]
    fn rejects_regex_predicates() {
        assert_unsupported(
            "MATCH (service:Service) WHERE service.name =~ '.*api' RETURN service.name",
        );
    }

    #[test]
    fn rejects_comparisons_without_property_operands() {
        assert_unsupported("MATCH (service:Service) WHERE 10 < 20 RETURN service.name");
    }

    #[test]
    fn compiles_grouped_count_projection() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN service.tier AS tier, count(*) AS services \
             ORDER BY tier",
        )
        .expect("grouped count query should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    },
                    alias: Some("tier".to_string()),
                },
                Projection::CountAll {
                    alias: "services".to_string(),
                },
            ]
        );
        assert_eq!(plan.order_by.len(), 1);
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
    fn compiles_count_property_projection() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN service.tier AS tier, count(service.name) AS named_services \
             ORDER BY named_services DESC",
        )
        .expect("count property query should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    },
                    alias: Some("tier".to_string()),
                },
                Projection::Aggregate {
                    function: super::AggregateFunction::Count,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    }),
                    distinct: false,
                    alias: "named_services".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::ProjectionAlias("named_services".to_string()),
                direction: OrderDirection::Descending,
            }]
        );
    }

    #[test]
    fn compiles_count_distinct_property_projection() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN count(DISTINCT service.tier) AS tier_count",
        )
        .expect("count distinct property query should compile");

        assert_eq!(
            plan.projections,
            vec![Projection::Aggregate {
                function: super::AggregateFunction::Count,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                distinct: true,
                alias: "tier_count".to_string(),
            }]
        );
    }

    #[test]
    fn compiles_numeric_aggregate_projections() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN service.tier AS tier, \
                    sum(service.risk) AS total_risk, \
                    avg(service.risk) AS average_risk, \
                    min(service.risk) AS lowest_risk, \
                    max(DISTINCT service.risk) AS highest_risk \
             ORDER BY average_risk DESC",
        )
        .expect("numeric aggregate query should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    },
                    alias: Some("tier".to_string()),
                },
                Projection::Aggregate {
                    function: super::AggregateFunction::Sum,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: false,
                    alias: "total_risk".to_string(),
                },
                Projection::Aggregate {
                    function: super::AggregateFunction::Avg,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: false,
                    alias: "average_risk".to_string(),
                },
                Projection::Aggregate {
                    function: super::AggregateFunction::Min,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: false,
                    alias: "lowest_risk".to_string(),
                },
                Projection::Aggregate {
                    function: super::AggregateFunction::Max,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: true,
                    alias: "highest_risk".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::ProjectionAlias("average_risk".to_string()),
                direction: OrderDirection::Descending,
            }]
        );
    }

    #[test]
    fn compiles_count_node_projection() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN count(service) AS services, count(DISTINCT service) AS distinct_services \
             ORDER BY services DESC",
        )
        .expect("count node query should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Aggregate {
                    function: super::AggregateFunction::Count,
                    target: AggregateTarget::VariableKey {
                        variable: "service".to_string(),
                    },
                    distinct: false,
                    alias: "services".to_string(),
                },
                Projection::Aggregate {
                    function: super::AggregateFunction::Count,
                    target: AggregateTarget::VariableKey {
                        variable: "service".to_string(),
                    },
                    distinct: true,
                    alias: "distinct_services".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::ProjectionAlias("services".to_string()),
                direction: OrderDirection::Descending,
            }]
        );
    }

    #[test]
    fn rejects_order_by_unknown_aliases() {
        assert_unsupported("MATCH (service:Service) RETURN service.name AS name ORDER BY missing");
    }

    #[test]
    fn rejects_unsupported_return_functions() {
        assert_unsupported("MATCH (service:Service) RETURN stdev(service.id) AS total");
    }

    #[test]
    fn compiles_order_by_aggregate_aliases() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN count(*) AS services \
             ORDER BY services DESC",
        )
        .expect("aggregate alias ordering should compile");

        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::ProjectionAlias("services".to_string()),
                direction: OrderDirection::Descending,
            }]
        );
    }

    fn assert_unsupported(cypher: &str) {
        let error = compile_cypher(cypher).expect_err("query should be rejected");
        assert!(
            error.to_string().contains("UNSUPPORTED_CYPHER"),
            "unexpected error: {error}"
        );
    }
}
