use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use decypher::ast::clause::{Match, ProjectionItem, Return, SortDirection, With};
use decypher::ast::expr::{
    BinaryOperator as CypherBinaryOperator, CaseExpression,
    ComparisonOperator as CypherComparisonOperator, Expression, FunctionInvocation,
    Literal as CypherLiteral, NumberLiteral, Parameter as CypherParameter, UnaryOperator,
};
use decypher::ast::names::Variable;
use decypher::ast::pattern::{
    LabelExpression, NodePattern as CypherNodePattern, PatternElement, Properties,
    Quantifier as CypherQuantifier, RangeLiteral as CypherRangeLiteral,
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
use regex::Regex;

use super::diagnostic::Diagnostic;
use super::ir::{
    AggregateFunction, AggregateTarget, ArithmeticOperator, ComparisonOperator, Direction,
    ElementIdPredicate, GraphPlan, KeyPredicate, Literal, NodePattern, OptionalMatchScope,
    OrderDirection, OrderExpression, OrderKey, PredicateExpression, PredicateRhs,
    PresencePredicate, Projection, ProjectionPredicate, ProjectionPredicateExpression,
    ProjectionPredicateRhs, PropertyKeyMembershipPredicate, PropertyPredicate, PropertyRef,
    RelationshipPattern, ScalarCaseAlternative, ScalarExpression, ScalarPredicate,
    ScalarPredicateRhs,
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
    variable_function_arguments: BTreeMap<(usize, usize), String>,
    parameters: BTreeMap<String, CypherParameterValue>,
}

impl CypherCompileContext {
    fn from_source_with_parameters(
        cypher: &str,
        parameters: BTreeMap<String, CypherParameterValue>,
    ) -> Self {
        Self {
            variable_function_arguments: collect_variable_function_arguments(cypher),
            parameters,
        }
    }

    fn variable_function_argument(&self, function: &FunctionInvocation) -> Option<&str> {
        self.variable_function_arguments
            .get(&(function.span.start, function.span.end))
            .map(String::as_str)
    }

    fn parameter_value(
        &self,
        parameter: &CypherParameter,
        path: impl Into<String>,
    ) -> Result<&CypherParameterValue, CoreError> {
        let path = path.into();
        let name = parameter.name.name.as_str();
        self.parameters.get(name).ok_or_else(|| {
            Diagnostic::new(
                "MISSING_PARAMETER",
                path,
                format!("Cypher parameter '${name}' was not provided"),
            )
            .into_core_error()
        })
    }
}

#[derive(Clone, Copy)]
enum PredicateCompileMode<'a> {
    Graph { plan: &'a GraphPlan },
    CaseWhen { plan: Option<&'a GraphPlan> },
}

impl<'a> PredicateCompileMode<'a> {
    fn graph_plan(self) -> Option<&'a GraphPlan> {
        match self {
            Self::Graph { plan } => Some(plan),
            Self::CaseWhen { plan } => plan,
        }
    }

    fn unsupported_predicate_message(self) -> &'static str {
        match self {
            Self::Graph { .. } => {
                "WHERE only supports graph property, id(), elementId(), labels(), keys(), exists(property), isEmpty(scalar), and supported scalar predicates combined with AND, OR, XOR, and NOT"
            }
            Self::CaseWhen { .. } => {
                "CASE WHEN predicates support property/scalar comparisons, static graph metadata predicates, IN literal lists, null checks, exists(property), isEmpty(scalar), boolean literals, and AND/OR/XOR/NOT"
            }
        }
    }

    fn unsupported_comparison_message(self) -> &'static str {
        match self {
            Self::Graph { .. } => {
                "comparisons must include at least one variable.property, id(variable), elementId(variable), type(relationship), or supported scalar expression operand"
            }
            Self::CaseWhen { .. } => {
                "CASE WHEN comparisons must include at least one variable.property, type(relationship), or supported scalar expression operand"
            }
        }
    }

    fn unsupported_in_message(self) -> &'static str {
        match self {
            Self::Graph { .. } => {
                "IN predicates require variable.property, id(variable), elementId(variable), type(relationship), supported scalar expression, '<label>' IN labels(node), or '<key>' IN keys(variable)"
            }
            Self::CaseWhen { .. } => {
                "CASE WHEN IN predicates require variable.property, type(relationship), supported scalar expression, '<label>' IN labels(node), or '<key>' IN keys(variable)"
            }
        }
    }

    fn unsupported_null_message(self) -> &'static str {
        match self {
            Self::Graph { .. } => {
                "IS NULL predicates require a graph variable, variable.property, id(variable), elementId(variable), type(relationship), or supported scalar expression"
            }
            Self::CaseWhen { .. } => {
                "CASE WHEN null checks require a graph variable, variable.property, id(variable), elementId(variable), or supported scalar expression operands"
            }
        }
    }

    fn graph_metadata_plan(self) -> Option<&'a GraphPlan> {
        match self {
            Self::Graph { plan } => Some(plan),
            Self::CaseWhen { .. } => None,
        }
    }

    fn static_metadata_plan(self) -> Option<&'a GraphPlan> {
        match self {
            Self::Graph { plan } => Some(plan),
            Self::CaseWhen { plan } => plan,
        }
    }
}

/// Runtime value that can be bound to a Cypher parameter in the supported subset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CypherParameterValue {
    /// Scalar literal parameter, usable where a literal expression is accepted.
    Literal(Literal),
    /// Scalar-list parameter, usable as the right-hand side of `IN`.
    List(Vec<Literal>),
}

/// Parses and compiles the Coral-supported read-only Cypher subset into a shared graph plan.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed or uses
/// Cypher/GQL features outside Coral's current read-only virtual graph subset.
pub fn compile_cypher(cypher: &str) -> Result<GraphPlan, CoreError> {
    compile_cypher_with_parameters(cypher, &BTreeMap::new())
}

/// Parses and compiles Cypher with typed parameter values into a shared graph plan.
///
/// Parameter values are bound before SQL lowering and only in positions where
/// the same literal or literal-list value is already supported by the read-only
/// Cypher subset.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed, uses
/// unsupported Cypher/GQL features, references a missing parameter, or binds a
/// list parameter where a scalar literal is required.
pub fn compile_cypher_with_parameters(
    cypher: &str,
    parameters: &BTreeMap<String, CypherParameterValue>,
) -> Result<GraphPlan, CoreError> {
    let query = decypher::parse(cypher).map_err(|error| {
        Diagnostic::new("CYPHER_PARSE_ERROR", "query", error.to_string()).into_core_error()
    })?;
    let context = CypherCompileContext::from_source_with_parameters(cypher, parameters.clone());
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
    compile_reading_clauses_into(&query.reading_clauses, "match", &mut plan, context)?;
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
    if let Some(plan) = compile_terminal_with_graph_modifiers(query, context)? {
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
    compile_reading_clauses_into(&part.reading_clauses, "parts[0].match", &mut plan, context)?;

    compile_terminal_with_clause(&part.with, &mut plan, context)?;
    apply_terminal_return_projection_aliases(return_clause, &mut plan.projections)?;
    apply_terminal_return_modifiers(return_clause, &mut plan, context)?;
    Ok(Some(plan))
}

fn with_requires_terminal_projection(with: &With) -> bool {
    with.items
        .iter()
        .any(|item| !matches!(&item.expression, Expression::Variable(_)))
}

fn compile_terminal_with_graph_modifiers(
    query: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<GraphPlan>, CoreError> {
    let terminal_modifier_candidate = query
        .parts
        .iter()
        .any(|part| with_has_row_modifiers(&part.with));
    if !terminal_modifier_candidate {
        return Ok(None);
    }
    let [part] = query.parts.as_slice() else {
        return Err(unsupported(
            "query.parts",
            "WITH ORDER BY, SKIP, and LIMIT over graph variables currently support exactly one MATCH ... WITH ... RETURN query part",
        ));
    };
    if with_requires_terminal_projection(&part.with) {
        return Ok(None);
    }
    if part.with.distinct {
        return Err(unsupported(
            "parts[0].with.distinct",
            "WITH DISTINCT over graph variables requires staged query planning and is not supported yet",
        ));
    }
    if !part.updating_clauses.is_empty() {
        return Err(unsupported(
            "parts[0].updating_clauses",
            "write clauses are not supported by Coral virtual graphs",
        ));
    }
    if !query.final_part.reading_clauses.is_empty() {
        return Err(unsupported(
            "final_part.reading_clauses",
            "WITH ORDER BY, SKIP, and LIMIT before another MATCH require staged query planning and are not supported yet",
        ));
    }

    let return_clause = return_clause_from_single_part(&query.final_part, "final_part")?;
    if with_has_row_modifiers(&part.with)
        && (return_clause.order.is_some()
            || return_clause.skip.is_some()
            || return_clause.limit.is_some())
    {
        return Err(unsupported(
            "final_part.return",
            "terminal WITH and RETURN cannot both define ORDER BY, SKIP, or LIMIT without staged query planning",
        ));
    }

    let mut plan = GraphPlan::default();
    compile_reading_clauses_into(&part.reading_clauses, "parts[0].match", &mut plan, context)?;
    if let Some(predicate) =
        apply_transparent_with_scope(&part.with, &mut plan, "parts[0].with", context)?
    {
        append_predicate_expression(predicate, &mut plan);
    }
    apply_terminal_graph_with_modifiers(&part.with, &mut plan, context)?;
    compile_return(return_clause, &mut plan, context)?;
    Ok(Some(plan))
}

fn with_has_row_modifiers(with: &With) -> bool {
    with.order.is_some() || with.skip.is_some() || with.limit.is_some()
}

fn apply_terminal_graph_with_modifiers(
    with: &With,
    plan: &mut GraphPlan,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    if let Some(order) = &with.order {
        for (index, item) in order.items.iter().enumerate() {
            plan.order_by.push(OrderKey {
                expression: compile_order_expression(
                    &item.expression,
                    &[],
                    plan,
                    context,
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
        plan.skip = Some(compile_skip(skip, "with.skip", context)?);
    }
    if let Some(limit) = &with.limit {
        plan.limit = Some(compile_limit(limit, "with.limit", context)?);
    }
    Ok(())
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
        let projection = compile_projection(item, format!("with.items[{index}]"), context, plan)?;
        plan.projections.push(projection);
    }
    if let Some(where_clause) = &with.where_clause {
        plan.post_projection_predicate = Some(compile_projection_predicate_expression(
            where_clause,
            "with.where",
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
        plan.skip = Some(compile_skip(skip, "with.skip", context)?);
    }
    if let Some(limit) = &with.limit {
        plan.limit = Some(compile_limit(limit, "with.limit", context)?);
    }
    Ok(())
}

fn apply_terminal_return_projection_aliases(
    return_clause: &Return,
    projections: &mut [Projection],
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
        .zip(projections.iter_mut())
        .enumerate()
    {
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
        if let Some(alias) = &item.alias {
            set_projection_output_alias(projection, variable_name(alias));
        }
    }
    Ok(())
}

fn set_projection_output_alias(projection: &mut Projection, alias: String) {
    match projection {
        Projection::Property {
            alias: projection_alias,
            ..
        } => *projection_alias = Some(alias),
        Projection::Key {
            alias: projection_alias,
            ..
        }
        | Projection::ElementId {
            alias: projection_alias,
            ..
        }
        | Projection::NodeLabels {
            alias: projection_alias,
            ..
        }
        | Projection::PropertyKeys {
            alias: projection_alias,
            ..
        }
        | Projection::RelationshipType {
            alias: projection_alias,
            ..
        }
        | Projection::Literal {
            alias: projection_alias,
            ..
        }
        | Projection::LiteralList {
            alias: projection_alias,
            ..
        }
        | Projection::Expression {
            alias: projection_alias,
            ..
        }
        | Projection::CountAll {
            alias: projection_alias,
        }
        | Projection::Aggregate {
            alias: projection_alias,
            ..
        } => *projection_alias = alias,
    }
}

fn apply_terminal_return_modifiers(
    return_clause: &Return,
    plan: &mut GraphPlan,
    context: &CypherCompileContext,
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
        plan.skip = Some(compile_skip(skip, "final_part.return.skip", context)?);
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
        plan.limit = Some(compile_limit(limit, "final_part.return.limit", context)?);
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
        Projection::Key { alias, .. }
        | Projection::ElementId { alias, .. }
        | Projection::NodeLabels { alias, .. }
        | Projection::PropertyKeys { alias, .. }
        | Projection::RelationshipType { alias, .. }
        | Projection::Literal { alias, .. }
        | Projection::LiteralList { alias, .. }
        | Projection::Expression { alias, .. }
        | Projection::CountAll { alias }
        | Projection::Aggregate { alias, .. } => Some(alias),
    }
}

fn compile_transparent_multi_part(
    query: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<GraphPlan, CoreError> {
    let mut plan = GraphPlan::default();
    for (index, part) in query.parts.iter().enumerate() {
        compile_transparent_multi_part_part(part, index, &mut plan, context)?;
    }

    match query.final_part.reading_clauses.as_slice() {
        [] => {}
        clauses => compile_reading_clauses_into(clauses, "final_part.match", &mut plan, context)?,
    }
    let return_clause = return_clause_from_single_part(&query.final_part, "final_part")?;
    compile_return(return_clause, &mut plan, context)?;
    Ok(plan)
}

fn compile_transparent_multi_part_part(
    part: &MultiPartQueryPart,
    index: usize,
    plan: &mut GraphPlan,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    if !part.updating_clauses.is_empty() {
        return Err(unsupported(
            format!("parts[{index}].updating_clauses"),
            "write clauses are not supported by Coral virtual graphs",
        ));
    }
    compile_reading_clauses_into(
        &part.reading_clauses,
        format!("parts[{index}].match"),
        plan,
        context,
    )?;
    if let Some(predicate) =
        validate_transparent_with(&part.with, plan, format!("parts[{index}].with"), context)?
    {
        append_predicate_expression(predicate, plan);
    }
    Ok(())
}

fn validate_transparent_with(
    with: &With,
    plan: &mut GraphPlan,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    if with.distinct {
        return Err(unsupported(
            format!("{path}.distinct"),
            "WITH DISTINCT requires staged query planning and is not supported yet",
        ));
    }
    if with.order.is_some() || with.skip.is_some() || with.limit.is_some() {
        return Err(unsupported(
            path.clone(),
            "WITH ORDER BY, SKIP, and LIMIT require staged query planning and are not supported yet",
        ));
    }
    apply_transparent_with_scope(with, plan, path, context)
}

fn apply_transparent_with_scope(
    with: &With,
    plan: &mut GraphPlan,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    if with.star {
        if !with.items.is_empty() {
            return Err(unsupported(
                format!("{path}.items"),
                "WITH * mixed with explicit projections requires scoped query planning and is not supported yet",
            ));
        }
        return compile_transparent_with_where(with, plan, path, context);
    }
    if with.items.is_empty() {
        return Err(unsupported(
            format!("{path}.items"),
            "WITH must carry every currently bound variable in this transparent subset",
        ));
    }

    let mut carried_inputs = BTreeSet::new();
    let mut carried_outputs = BTreeSet::new();
    let mut renames = BTreeMap::new();
    for (index, item) in with.items.iter().enumerate() {
        let Expression::Variable(variable) = &item.expression else {
            return Err(unsupported(
                format!("{path}.items[{index}].expression"),
                "transparent WITH only supports pass-through graph variables",
            ));
        };
        let input = variable_name(variable);
        let output = item
            .alias
            .as_ref()
            .map(validate_variable)
            .transpose()?
            .unwrap_or_else(|| input.clone());
        if !carried_inputs.insert(input.clone()) {
            return Err(unsupported(
                format!("{path}.items[{index}].expression"),
                format!("WITH carries graph variable '{input}' more than once"),
            ));
        }
        if !carried_outputs.insert(output.clone()) {
            return Err(unsupported(
                format!("{path}.items[{index}].alias"),
                format!("WITH output variable '{output}' is defined more than once"),
            ));
        }
        renames.insert(input, output);
    }

    let bound = bound_graph_variables(plan);
    if carried_inputs != bound {
        return Err(unsupported(
            format!("{path}.items"),
            "transparent WITH must carry every currently bound graph variable without dropping or adding variables",
        ));
    }
    if renames.iter().any(|(from, to)| from != to) {
        rename_graph_plan_variables(plan, &renames);
    }

    compile_transparent_with_where(with, plan, path, context)
}

fn compile_transparent_with_where(
    with: &With,
    plan: &GraphPlan,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    with.where_clause
        .as_ref()
        .map(|where_clause| {
            compile_predicate_expression(where_clause, format!("{path}.where"), plan, context)
        })
        .transpose()
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

fn rename_graph_plan_variables(plan: &mut GraphPlan, renames: &BTreeMap<String, String>) {
    for node in &mut plan.nodes {
        rename_string(&mut node.variable, renames);
    }
    for relationship in &mut plan.relationships {
        if let Some(variable) = &mut relationship.variable {
            rename_string(variable, renames);
        }
        rename_string(&mut relationship.left, renames);
        rename_string(&mut relationship.right, renames);
    }
    for projection in &mut plan.projections {
        rename_projection_variables(projection, renames);
    }
    for predicate in &mut plan.predicates {
        rename_property_predicate_variables(predicate, renames);
    }
    if let Some(predicate) = &mut plan.predicate {
        rename_predicate_expression_variables(predicate, renames);
    }
    for optional_match in &mut plan.optional_matches {
        if let Some(predicate) = &mut optional_match.predicate {
            rename_predicate_expression_variables(predicate, renames);
        }
    }
    for order_key in &mut plan.order_by {
        rename_order_expression_variables(&mut order_key.expression, renames);
    }
}

fn rename_projection_variables(projection: &mut Projection, renames: &BTreeMap<String, String>) {
    match projection {
        Projection::Property { property, .. } => rename_property_ref_variables(property, renames),
        Projection::Key { variable, .. }
        | Projection::ElementId { variable, .. }
        | Projection::RelationshipType { variable, .. }
        | Projection::NodeLabels { variable, .. }
        | Projection::PropertyKeys { variable, .. } => rename_string(variable, renames),
        Projection::Expression { expression, .. } => {
            rename_scalar_expression_variables(expression, renames);
        }
        Projection::Aggregate { target, .. } => rename_aggregate_target_variables(target, renames),
        Projection::Literal { .. }
        | Projection::LiteralList { .. }
        | Projection::CountAll { .. } => {}
    }
}

fn rename_aggregate_target_variables(
    target: &mut AggregateTarget,
    renames: &BTreeMap<String, String>,
) {
    match target {
        AggregateTarget::Property(property) => rename_property_ref_variables(property, renames),
        AggregateTarget::VariableKey { variable } => rename_string(variable, renames),
    }
}

fn rename_order_expression_variables(
    expression: &mut OrderExpression,
    renames: &BTreeMap<String, String>,
) {
    match expression {
        OrderExpression::Property(property) => rename_property_ref_variables(property, renames),
        OrderExpression::Key { variable }
        | OrderExpression::ElementId { variable }
        | OrderExpression::RelationshipType { variable, .. }
        | OrderExpression::NodeLabels { variable, .. }
        | OrderExpression::PropertyKeys { variable } => rename_string(variable, renames),
        OrderExpression::Scalar(expression) => {
            rename_scalar_expression_variables(expression, renames);
        }
        OrderExpression::Literal(_) | OrderExpression::ProjectionAlias(_) => {}
    }
}

fn rename_predicate_expression_variables(
    expression: &mut PredicateExpression,
    renames: &BTreeMap<String, String>,
) {
    match expression {
        PredicateExpression::Boolean(_) => {}
        PredicateExpression::Comparison(predicate) => {
            rename_property_predicate_variables(predicate, renames);
        }
        PredicateExpression::KeyComparison(predicate) => {
            rename_string(&mut predicate.variable, renames);
            rename_predicate_rhs_variables(&mut predicate.rhs, renames);
        }
        PredicateExpression::ElementIdComparison(predicate) => {
            rename_string(&mut predicate.variable, renames);
            rename_predicate_rhs_variables(&mut predicate.rhs, renames);
        }
        PredicateExpression::Presence(predicate) => {
            rename_string(&mut predicate.variable, renames);
        }
        PredicateExpression::PropertyKeyMembership(predicate) => {
            rename_string(&mut predicate.variable, renames);
        }
        PredicateExpression::ScalarComparison(predicate) => {
            rename_scalar_expression_variables(&mut predicate.lhs, renames);
            rename_scalar_predicate_rhs_variables(&mut predicate.rhs, renames);
        }
        PredicateExpression::And { left, right }
        | PredicateExpression::Or { left, right }
        | PredicateExpression::Xor { left, right } => {
            rename_predicate_expression_variables(left, renames);
            rename_predicate_expression_variables(right, renames);
        }
        PredicateExpression::Not { expression } => {
            rename_predicate_expression_variables(expression, renames);
        }
    }
}

fn rename_property_predicate_variables(
    predicate: &mut PropertyPredicate,
    renames: &BTreeMap<String, String>,
) {
    rename_property_ref_variables(&mut predicate.property, renames);
    rename_predicate_rhs_variables(&mut predicate.rhs, renames);
}

fn rename_predicate_rhs_variables(rhs: &mut PredicateRhs, renames: &BTreeMap<String, String>) {
    match rhs {
        PredicateRhs::Property(property) => rename_property_ref_variables(property, renames),
        PredicateRhs::Key { variable } | PredicateRhs::ElementId { variable } => {
            rename_string(variable, renames);
        }
        PredicateRhs::Literal(_) | PredicateRhs::List(_) => {}
    }
}

fn rename_scalar_predicate_rhs_variables(
    rhs: &mut ScalarPredicateRhs,
    renames: &BTreeMap<String, String>,
) {
    match rhs {
        ScalarPredicateRhs::Expression(expression) => {
            rename_scalar_expression_variables(expression, renames);
        }
        ScalarPredicateRhs::List(_) => {}
    }
}

fn rename_scalar_expression_variables(
    expression: &mut ScalarExpression,
    renames: &BTreeMap<String, String>,
) {
    if let Some(expression) = unary_scalar_expression_operand_mut(expression) {
        rename_scalar_expression_variables(expression, renames);
        return;
    }

    rename_non_unary_scalar_expression_variables(expression, renames);
}

fn rename_non_unary_scalar_expression_variables(
    expression: &mut ScalarExpression,
    renames: &BTreeMap<String, String>,
) {
    match expression {
        ScalarExpression::Property(property) => rename_property_ref_variables(property, renames),
        ScalarExpression::Literal(_) => {}
        ScalarExpression::Predicate(predicate) => {
            rename_predicate_expression_variables(predicate, renames);
        }
        ScalarExpression::Coalesce { expressions } => {
            for expression in expressions {
                rename_scalar_expression_variables(expression, renames);
            }
        }
        ScalarExpression::NullIf { expression, value } => {
            rename_scalar_expression_variables(expression, renames);
            rename_scalar_expression_variables(value, renames);
        }
        ScalarExpression::Round { expression, places } => {
            rename_scalar_expression_variables(expression, renames);
            if let Some(places) = places {
                rename_scalar_expression_variables(places, renames);
            }
        }
        ScalarExpression::Left { expression, count }
        | ScalarExpression::Right { expression, count } => {
            rename_scalar_expression_variables(expression, renames);
            rename_scalar_expression_variables(count, renames);
        }
        ScalarExpression::Replace {
            expression,
            search,
            replacement,
        } => {
            rename_scalar_expression_variables(expression, renames);
            rename_scalar_expression_variables(search, renames);
            rename_scalar_expression_variables(replacement, renames);
        }
        ScalarExpression::Substring {
            expression,
            start,
            length,
        } => {
            rename_scalar_expression_variables(expression, renames);
            rename_scalar_expression_variables(start, renames);
            if let Some(length) = length {
                rename_scalar_expression_variables(length, renames);
            }
        }
        ScalarExpression::Arithmetic { left, right, .. } => {
            rename_scalar_expression_variables(left, renames);
            rename_scalar_expression_variables(right, renames);
        }
        ScalarExpression::Atan2 { y, x } => {
            rename_scalar_expression_variables(y, renames);
            rename_scalar_expression_variables(x, renames);
        }
        ScalarExpression::Case {
            alternatives,
            else_expression,
        } => {
            rename_case_expression_variables(alternatives, else_expression.as_deref_mut(), renames);
        }
        ScalarExpression::ToString { .. }
        | ScalarExpression::ToInteger { .. }
        | ScalarExpression::ToFloat { .. }
        | ScalarExpression::ToBoolean { .. }
        | ScalarExpression::ToStringOrNull { .. }
        | ScalarExpression::ToIntegerOrNull { .. }
        | ScalarExpression::ToFloatOrNull { .. }
        | ScalarExpression::ToBooleanOrNull { .. }
        | ScalarExpression::ToLower { .. }
        | ScalarExpression::ToUpper { .. }
        | ScalarExpression::Trim { .. }
        | ScalarExpression::LTrim { .. }
        | ScalarExpression::RTrim { .. }
        | ScalarExpression::CharacterLength { .. }
        | ScalarExpression::Reverse { .. }
        | ScalarExpression::Abs { .. }
        | ScalarExpression::Ceil { .. }
        | ScalarExpression::Floor { .. }
        | ScalarExpression::Sqrt { .. }
        | ScalarExpression::Sign { .. }
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
        | ScalarExpression::Degrees { .. }
        | ScalarExpression::Radians { .. }
        | ScalarExpression::Negate { .. } => {
            unreachable!("unary scalar expressions handled before structural rename")
        }
    }
}

fn rename_case_expression_variables(
    alternatives: &mut [ScalarCaseAlternative],
    else_expression: Option<&mut ScalarExpression>,
    renames: &BTreeMap<String, String>,
) {
    for alternative in alternatives {
        rename_predicate_expression_variables(&mut alternative.when, renames);
        rename_scalar_expression_variables(&mut alternative.then, renames);
    }
    if let Some(else_expression) = else_expression {
        rename_scalar_expression_variables(else_expression, renames);
    }
}

fn unary_scalar_expression_operand_mut(
    expression: &mut ScalarExpression,
) -> Option<&mut ScalarExpression> {
    match expression {
        ScalarExpression::ToString { expression }
        | ScalarExpression::ToInteger { expression }
        | ScalarExpression::ToFloat { expression }
        | ScalarExpression::ToBoolean { expression }
        | ScalarExpression::ToStringOrNull { expression }
        | ScalarExpression::ToIntegerOrNull { expression }
        | ScalarExpression::ToFloatOrNull { expression }
        | ScalarExpression::ToBooleanOrNull { expression }
        | ScalarExpression::ToLower { expression }
        | ScalarExpression::ToUpper { expression }
        | ScalarExpression::Trim { expression }
        | ScalarExpression::LTrim { expression }
        | ScalarExpression::RTrim { expression }
        | ScalarExpression::CharacterLength { expression }
        | ScalarExpression::Reverse { expression }
        | ScalarExpression::Abs { expression }
        | ScalarExpression::Ceil { expression }
        | ScalarExpression::Floor { expression }
        | ScalarExpression::Sqrt { expression }
        | ScalarExpression::Sign { expression }
        | ScalarExpression::Exp { expression }
        | ScalarExpression::Log { expression }
        | ScalarExpression::Log10 { expression }
        | ScalarExpression::Sin { expression }
        | ScalarExpression::Cos { expression }
        | ScalarExpression::Tan { expression }
        | ScalarExpression::Cot { expression }
        | ScalarExpression::Asin { expression }
        | ScalarExpression::Acos { expression }
        | ScalarExpression::Atan { expression }
        | ScalarExpression::Degrees { expression }
        | ScalarExpression::Radians { expression }
        | ScalarExpression::Negate { expression } => Some(expression),
        _ => None,
    }
}

fn rename_property_ref_variables(property: &mut PropertyRef, renames: &BTreeMap<String, String>) {
    rename_string(&mut property.variable, renames);
}

fn rename_string(value: &mut String, renames: &BTreeMap<String, String>) {
    if let Some(replacement) = renames.get(value.as_str()) {
        *value = replacement.clone();
    }
}

fn compile_reading_clauses_into(
    reading_clauses: &[ReadingClause],
    path: impl Into<String>,
    plan: &mut GraphPlan,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    let path = path.into();
    if reading_clauses.is_empty() {
        return Err(unsupported(
            path,
            "at least one MATCH clause is required before RETURN",
        ));
    }

    let mut saw_optional_match = false;
    for (index, clause) in reading_clauses.iter().enumerate() {
        match clause {
            ReadingClause::Match(match_clause) => {
                if saw_optional_match && !match_clause.optional {
                    return Err(unsupported(
                        format!("{path}[{index}]"),
                        "MATCH after OPTIONAL MATCH requires staged query planning and is not supported yet",
                    ));
                }
                if match_clause.optional {
                    saw_optional_match = true;
                }
                let predicate_start = plan.predicates.len();
                let relationship_start = plan.relationships.len();
                compile_match_into(match_clause, plan, context)?;
                if match_clause.optional {
                    let predicate = match_clause
                        .where_clause
                        .as_ref()
                        .map(|where_clause| {
                            compile_predicate_expression(
                                where_clause,
                                format!("{path}[{index}].where"),
                                plan,
                                context,
                            )
                        })
                        .transpose()?;
                    attach_optional_match_scope(
                        plan,
                        relationship_start,
                        predicate_start,
                        predicate,
                        format!("{path}[{index}]"),
                    )?;
                } else if let Some(where_clause) = &match_clause.where_clause {
                    let predicate = compile_predicate_expression(
                        where_clause,
                        format!("{path}[{index}].where"),
                        plan,
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

fn attach_optional_match_scope(
    plan: &mut GraphPlan,
    relationship_start: usize,
    predicate_start: usize,
    predicate: Option<PredicateExpression>,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    let relationship_indices = (relationship_start..plan.relationships.len()).collect::<Vec<_>>();
    let inline_predicates = plan.predicates.drain(predicate_start..).collect::<Vec<_>>();
    let predicate = combine_optional_predicates(inline_predicates, predicate);
    if relationship_indices.is_empty() && predicate.is_some() {
        return Err(unsupported(
            path,
            "OPTIONAL MATCH predicates currently require a relationship pattern",
        ));
    }
    if relationship_indices.is_empty() {
        return Ok(());
    }

    if predicate.is_some() && relationship_indices.len() != 1 {
        return Err(unsupported(
            path,
            "OPTIONAL MATCH predicates currently require a single relationship pattern",
        ));
    }
    if predicate.is_some()
        && relationship_indices.iter().any(|index| {
            plan.relationships
                .get(*index)
                .is_some_and(|pattern| pattern.direction == Direction::Undirected)
        })
    {
        return Err(unsupported(
            path,
            "OPTIONAL MATCH predicates on undirected relationships require orientation-aware join grouping and are not supported yet",
        ));
    }

    plan.optional_matches.push(OptionalMatchScope {
        relationship_indices,
        predicate,
    });
    Ok(())
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

fn compile_match_into(
    match_clause: &Match,
    plan: &mut GraphPlan,
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
    if match_clause.optional
        && !match_clause
            .pattern
            .parts
            .iter()
            .any(|part| pattern_part_uses_bound_node(part, &initially_bound_nodes))
    {
        return Err(unsupported(
            "match.pattern",
            "OPTIONAL MATCH must be anchored to a previously bound node variable",
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
            fresh_internal_node_variable(plan, part_index, 0),
            format!("match.pattern.parts[{part_index}].nodes[0]"),
            context,
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
            let next_node = compile_node(
                &chain.node,
                plan,
                fresh_internal_node_variable(plan, part_index, chain_index + 1),
                node_path,
                context,
            )?;
            let next_variable = next_node.variable.clone();
            let relationship_index = plan.relationships.len();
            let relationship_path =
                format!("match.pattern.parts[{part_index}].relationships[{chain_index}]");
            let relationship = compile_relationship(
                &chain.relationship,
                (&previous_variable, &next_variable),
                relationship_index,
                plan,
                relationship_path,
                context,
            )?;
            previous_variable = next_variable;
            plan.predicates.extend(next_node.predicates);
            if let Some(pattern) = next_node.pattern {
                plan.nodes.push(pattern);
            }
            plan.predicates.extend(relationship.predicates);
            if match_clause.optional {
                plan.optional_relationships.push(relationship_index);
            }
            plan.relationships.push(relationship.pattern);
        }
    }

    Ok(())
}

fn pattern_part_uses_bound_node(
    pattern_part: &decypher::ast::pattern::PatternPart,
    bound_nodes: &BTreeSet<&str>,
) -> bool {
    let PatternElement::Path { start, chains } = &pattern_part.anonymous.element else {
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
    anonymous_variable: String,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<CompiledNode, CoreError> {
    let path = path.into();
    let is_anonymous = pattern.variable.is_none();
    let variable = match pattern.variable.as_ref() {
        Some(variable) => validate_variable(variable)?,
        None => anonymous_variable,
    };
    let label = optional_single_static_label(&pattern.labels, format!("{path}.labels"))?;
    if is_anonymous && label.is_none() {
        return Err(unsupported(
            format!("{path}.labels"),
            "anonymous node patterns require exactly one static label",
        ));
    }
    let predicates = pattern.properties.as_ref().map_or_else(
        || Ok(Vec::new()),
        |properties| {
            compile_inline_properties(properties, &variable, format!("{path}.properties"), context)
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
            rhs: PredicateRhs::Literal(compile_literal(
                expression,
                format!("{path}.entries[{index}].value"),
                context,
            )?),
        });
    }
    Ok(predicates)
}

fn compile_relationship(
    pattern: &CypherRelationshipPattern,
    endpoints: (&str, &str),
    index: usize,
    plan: &GraphPlan,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<CompiledRelationship, CoreError> {
    let path = path.into();
    let (left, right) = endpoints;
    if let Some(quantifier) = &pattern.quantifier {
        validate_exact_one_quantifier(quantifier, format!("{path}.quantifier"))?;
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
    if let Some(range) = &detail.range {
        validate_exact_one_range(range, format!("{path}.range"))?;
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
            compile_inline_properties(properties, variable, format!("{path}.properties"), context)?
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

fn validate_exact_one_quantifier(
    quantifier: &CypherQuantifier,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    validate_exact_one_bounds(
        quantifier.start,
        quantifier.end,
        path,
        "relationship quantifiers other than exact {1} are not supported yet",
    )
}

fn validate_exact_one_range(
    range: &CypherRangeLiteral,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    validate_exact_one_bounds(
        range.start,
        range.end,
        path,
        "variable-length relationship ranges other than exact *1 are not supported yet",
    )
}

fn validate_exact_one_bounds(
    start: Option<i64>,
    end: Option<i64>,
    path: impl Into<String>,
    message: &'static str,
) -> Result<(), CoreError> {
    if start == Some(1) && end == Some(1) {
        return Ok(());
    }
    Err(unsupported(path, message))
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
        plan.skip = Some(compile_skip(skip, "return.skip", context)?);
    }
    if return_clause.items.is_empty() {
        return Err(unsupported(
            "return.items",
            "RETURN must include at least one projection",
        ));
    }

    for (index, item) in return_clause.items.iter().enumerate() {
        let projection = compile_projection(item, format!("return.items[{index}]"), context, plan)?;
        plan.projections.push(projection);
    }

    if let Some(order) = &return_clause.order {
        for (index, item) in order.items.iter().enumerate() {
            plan.order_by.push(OrderKey {
                expression: compile_order_expression(
                    &item.expression,
                    &plan.projections,
                    plan,
                    context,
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
        plan.limit = Some(compile_limit(limit, "return.limit", context)?);
    }

    Ok(())
}

fn compile_order_expression(
    expression: &Expression,
    projections: &[Projection],
    plan: &GraphPlan,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_order_expression(inner, projections, plan, context, path)
        }
        Expression::Variable(variable) => {
            projection_order_expression_for_alias(variable, projections, path)
        }
        Expression::CountStar { .. } => {
            count_star_order_expression_for_projection(projections, path)
        }
        expression if is_literal_expression(expression) => Ok(OrderExpression::Literal(
            compile_literal(expression, path, context)?,
        )),
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            ..
        } => compile_arithmetic_order_expression(expression, path, context),
        Expression::BinaryOp { .. } => {
            if let Some(expression) =
                compile_optional_boolean_scalar_expression(expression, path.clone(), plan, context)?
            {
                Ok(OrderExpression::Scalar(expression))
            } else {
                compile_arithmetic_order_expression(expression, path, context)
            }
        }
        expression if is_boolean_scalar_expression(expression) => Ok(OrderExpression::Scalar(
            compile_boolean_scalar_expression(expression, path, plan, context)?,
        )),
        Expression::Case(case) => compile_case_order_expression(case, path, plan, context),
        Expression::FunctionCall(function) if is_id_function(function) => {
            compile_id_order_expression(function, path, plan, context)
        }
        Expression::FunctionCall(function) if is_element_id_function(function) => {
            compile_element_id_order_expression(function, path, plan, context)
        }
        Expression::FunctionCall(function) if is_type_function(function) => {
            compile_type_order_expression(function, path, plan, context)
        }
        Expression::FunctionCall(function) if is_labels_function(function) => {
            compile_labels_order_expression(function, path, plan, context)
        }
        Expression::FunctionCall(function) if is_keys_function(function) => {
            compile_keys_order_expression(function, path, plan, context)
        }
        Expression::FunctionCall(function) => {
            if let Some(expression) =
                compile_scalar_function_expression(function, path.clone(), context)?
            {
                return Ok(OrderExpression::Scalar(expression));
            }
            if compile_aggregate_function(function).is_some() {
                return aggregate_order_expression_for_projection(
                    function,
                    projections,
                    path,
                    context,
                );
            }
            Ok(OrderExpression::Property(compile_property_ref(
                expression, path,
            )?))
        }
        _ => Ok(OrderExpression::Property(compile_property_ref(
            expression, path,
        )?)),
    }
}

fn count_star_order_expression_for_projection(
    projections: &[Projection],
    path: impl Into<String>,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    let aliases = projections
        .iter()
        .filter_map(|projection| match projection {
            Projection::CountAll { alias } => Some(alias.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    projection_alias_for_matching_order_expression(&aliases, path, "count(*)")
}

fn aggregate_order_expression_for_projection(
    function: &FunctionInvocation,
    projections: &[Projection],
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    let function_kind = compile_aggregate_function(function).ok_or_else(|| {
        unsupported(
            path.clone(),
            format!(
                "ORDER BY function '{}' is not supported yet",
                qualified_function_name(function)
            ),
        )
    })?;
    let target = compile_function_aggregate_target(function, function_kind, &path, context)?;
    let aliases = projections
        .iter()
        .filter_map(|projection| match projection {
            Projection::Aggregate {
                function: projection_function,
                target: projection_target,
                distinct,
                alias,
            } if *projection_function == function_kind
                && projection_target == &target
                && *distinct == function.distinct =>
            {
                Some(alias.clone())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    projection_alias_for_matching_order_expression(
        &aliases,
        path,
        &format!("{}()", aggregate_function_name(function_kind)),
    )
}

fn projection_alias_for_matching_order_expression(
    aliases: &[String],
    path: String,
    expression: &str,
) -> Result<OrderExpression, CoreError> {
    match aliases {
        [alias] => Ok(OrderExpression::ProjectionAlias(alias.clone())),
        [] => Err(unsupported(
            path,
            format!("ORDER BY {expression} must match a RETURN aggregate projection"),
        )),
        _ => Err(unsupported(
            path,
            format!("ORDER BY {expression} is ambiguous because multiple RETURN projections match"),
        )),
    }
}

fn compile_id_order_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    let variable = compile_single_variable_function_argument(
        function,
        format!("{path}.arguments"),
        "id() supports exactly one graph variable argument",
        context,
    )?;
    if !plan_uses_variable(plan, &variable) {
        return Err(unsupported(
            format!("{path}.arguments[0]"),
            format!("id() argument '{variable}' is not a bound graph variable"),
        ));
    }
    Ok(OrderExpression::Key { variable })
}

fn compile_element_id_order_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    let variable = compile_element_id_variable(function, path, plan, context)?;
    Ok(OrderExpression::ElementId { variable })
}

fn compile_type_order_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    let variable = compile_single_variable_function_argument(
        function,
        format!("{path}.arguments"),
        "type() supports exactly one relationship variable argument",
        context,
    )?;
    let relationship = plan
        .relationships
        .iter()
        .find(|relationship| relationship.variable.as_deref() == Some(variable.as_str()))
        .ok_or_else(|| {
            unsupported(
                format!("{path}.arguments[0]"),
                format!("type() argument '{variable}' is not a named relationship variable"),
            )
        })?;
    Ok(OrderExpression::RelationshipType {
        variable,
        relationship_type: relationship.relationship_type.clone(),
    })
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
            | Projection::Key {
                alias: projection_alias,
                ..
            }
            | Projection::ElementId {
                alias: projection_alias,
                ..
            }
            | Projection::NodeLabels {
                alias: projection_alias,
                ..
            }
            | Projection::PropertyKeys {
                alias: projection_alias,
                ..
            }
            | Projection::RelationshipType {
                alias: projection_alias,
                ..
            }
            | Projection::Literal {
                alias: projection_alias,
                ..
            }
            | Projection::LiteralList {
                alias: projection_alias,
                ..
            }
            | Projection::Expression {
                alias: projection_alias,
                ..
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
    plan: &GraphPlan,
) -> Result<Projection, CoreError> {
    let path = path.into();
    match &item.expression {
        Expression::CountStar { .. } => Ok(Projection::CountAll {
            alias: item
                .alias
                .as_ref()
                .map_or_else(|| "count".to_string(), variable_name),
        }),
        expression if is_literal_projection_expression(expression) => {
            compile_literal_projection(expression, item, path, context)
        }
        expression if is_boolean_scalar_expression(expression) => {
            compile_boolean_scalar_projection(expression, item, path, plan, context)
        }
        Expression::Parenthesized(inner) if is_arithmetic_expression(inner) => {
            compile_arithmetic_projection(item, path, context)
        }
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            ..
        }
        | Expression::BinaryOp { .. } => compile_arithmetic_projection(item, path, context),
        Expression::Case(case) => compile_case_projection(case, item, path, plan, context),
        Expression::FunctionCall(function) if is_id_function(function) => {
            compile_id_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_element_id_function(function) => {
            compile_element_id_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_type_function(function) => {
            compile_type_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_labels_function(function) => {
            compile_labels_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_keys_function(function) => {
            compile_keys_projection(function, item, path, context)
        }
        Expression::FunctionCall(function) => {
            if let Some(projection) =
                compile_scalar_function_projection(function, item, path.clone(), context)?
            {
                return Ok(projection);
            }
            if compile_aggregate_function(function).is_some() {
                return compile_aggregate_projection(function, item, path, context);
            }
            Err(unsupported(
                format!("{path}.expression"),
                format!(
                    "RETURN function '{}' is not supported yet",
                    qualified_function_name(function)
                ),
            ))
        }
        expression => Ok(Projection::Property {
            property: compile_property_ref(expression, format!("{path}.expression"))?,
            alias: item.alias.as_ref().map(variable_name),
        }),
    }
}

fn compile_literal_projection(
    expression: &Expression,
    item: &ProjectionItem,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    match compile_projection_literal(expression, format!("{path}.expression"), context)? {
        ProjectionLiteral::Scalar(literal) => Ok(Projection::Literal {
            literal,
            alias: item
                .alias
                .as_ref()
                .map_or_else(|| "literal".to_string(), variable_name),
        }),
        ProjectionLiteral::List(literals) => Ok(Projection::LiteralList {
            literals,
            alias: item
                .alias
                .as_ref()
                .map_or_else(|| "list".to_string(), variable_name),
        }),
    }
}

fn compile_arithmetic_projection(
    item: &ProjectionItem,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    Ok(Projection::Expression {
        expression: compile_scalar_expression(
            &item.expression,
            format!("{path}.expression"),
            context,
        )?,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "expression".to_string(), variable_name),
    })
}

fn compile_boolean_scalar_projection(
    expression: &Expression,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    Ok(Projection::Expression {
        expression: compile_boolean_scalar_expression(
            expression,
            format!("{path}.expression"),
            plan,
            context,
        )?,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "expression".to_string(), variable_name),
    })
}

fn compile_case_projection(
    case: &CaseExpression,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    Ok(Projection::Expression {
        expression: compile_case_scalar_expression_with_plan(
            case,
            format!("{path}.expression"),
            plan,
            context,
        )?,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "case".to_string(), variable_name),
    })
}

fn compile_scalar_function_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Option<Projection>, CoreError> {
    let path = path.into();
    let Some(expression) =
        compile_scalar_function_expression(function, format!("{path}.expression"), context)?
    else {
        return Ok(None);
    };
    Ok(Some(Projection::Expression {
        expression,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| default_scalar_function_alias(function), variable_name),
    }))
}

fn default_scalar_function_alias(function: &FunctionInvocation) -> String {
    if is_character_length_function(function) {
        return "size".to_string();
    }
    qualified_function_name(function)
}

fn compile_coalesce_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    if function.arguments.len() < 2 {
        return Err(unsupported(
            format!("{path}.arguments"),
            "coalesce() requires at least two arguments",
        ));
    }
    let expressions = function
        .arguments
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            compile_scalar_expression(expression, format!("{path}.arguments[{index}]"), context)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ScalarExpression::Coalesce { expressions })
}

fn compile_null_if_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (expression, value) =
        compile_two_scalar_function_arguments(function, path, "nullIf", context)?;
    Ok(ScalarExpression::NullIf {
        expression: Box::new(expression),
        value: Box::new(value),
    })
}

fn compile_to_string_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToString {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "toString", context,
        )?),
    })
}

fn compile_to_integer_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToInteger {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toInteger",
            context,
        )?),
    })
}

fn compile_to_float_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToFloat {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "toFloat", context,
        )?),
    })
}

fn compile_to_boolean_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToBoolean {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toBoolean",
            context,
        )?),
    })
}

fn compile_to_string_or_null_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToStringOrNull {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toStringOrNull",
            context,
        )?),
    })
}

fn compile_to_integer_or_null_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToIntegerOrNull {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toIntegerOrNull",
            context,
        )?),
    })
}

fn compile_to_float_or_null_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToFloatOrNull {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toFloatOrNull",
            context,
        )?),
    })
}

fn compile_to_boolean_or_null_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToBooleanOrNull {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toBooleanOrNull",
            context,
        )?),
    })
}

fn compile_to_lower_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let function_name = single_segment_function_name(function).unwrap_or("toLower");
    Ok(ScalarExpression::ToLower {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            function_name,
            context,
        )?),
    })
}

fn compile_to_upper_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let function_name = single_segment_function_name(function).unwrap_or("toUpper");
    Ok(ScalarExpression::ToUpper {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            function_name,
            context,
        )?),
    })
}

fn compile_trim_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let function_name = single_segment_function_name(function).unwrap_or("trim");
    Ok(ScalarExpression::Trim {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            function_name,
            context,
        )?),
    })
}

fn compile_ltrim_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::LTrim {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "lTrim", context,
        )?),
    })
}

fn compile_rtrim_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::RTrim {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "rTrim", context,
        )?),
    })
}

fn compile_replace_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let [expression, search, replacement] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "replace() requires exactly three arguments",
        ));
    };
    Ok(ScalarExpression::Replace {
        expression: Box::new(compile_scalar_expression(
            expression,
            format!("{path}.arguments[0]"),
            context,
        )?),
        search: Box::new(compile_scalar_expression(
            search,
            format!("{path}.arguments[1]"),
            context,
        )?),
        replacement: Box::new(compile_scalar_expression(
            replacement,
            format!("{path}.arguments[2]"),
            context,
        )?),
    })
}

fn compile_character_length_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let function_name = qualified_function_name(function);
    Ok(ScalarExpression::CharacterLength {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            function_name.as_str(),
            context,
        )?),
    })
}

fn compile_substring_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match function.arguments.as_slice() {
        [expression, start] => Ok(ScalarExpression::Substring {
            expression: Box::new(compile_scalar_expression(
                expression,
                format!("{path}.arguments[0]"),
                context,
            )?),
            start: Box::new(compile_scalar_expression(
                start,
                format!("{path}.arguments[1]"),
                context,
            )?),
            length: None,
        }),
        [expression, start, length] => Ok(ScalarExpression::Substring {
            expression: Box::new(compile_scalar_expression(
                expression,
                format!("{path}.arguments[0]"),
                context,
            )?),
            start: Box::new(compile_scalar_expression(
                start,
                format!("{path}.arguments[1]"),
                context,
            )?),
            length: Some(Box::new(compile_scalar_expression(
                length,
                format!("{path}.arguments[2]"),
                context,
            )?)),
        }),
        _ => Err(unsupported(
            format!("{path}.arguments"),
            "substring() requires exactly two or three arguments",
        )),
    }
}

fn compile_left_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (expression, count) =
        compile_two_scalar_function_arguments(function, path, "left", context)?;
    Ok(ScalarExpression::Left {
        expression: Box::new(expression),
        count: Box::new(count),
    })
}

fn compile_right_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (expression, count) =
        compile_two_scalar_function_arguments(function, path, "right", context)?;
    Ok(ScalarExpression::Right {
        expression: Box::new(expression),
        count: Box::new(count),
    })
}

fn compile_reverse_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Reverse {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "reverse", context,
        )?),
    })
}

fn compile_abs_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Abs {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "abs", context,
        )?),
    })
}

fn compile_ceil_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Ceil {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "ceil", context,
        )?),
    })
}

fn compile_floor_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Floor {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "floor", context,
        )?),
    })
}

fn compile_round_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match function.arguments.as_slice() {
        [expression] => Ok(ScalarExpression::Round {
            expression: Box::new(compile_scalar_expression(
                expression,
                format!("{path}.arguments[0]"),
                context,
            )?),
            places: None,
        }),
        [expression, places] => Ok(ScalarExpression::Round {
            expression: Box::new(compile_scalar_expression(
                expression,
                format!("{path}.arguments[0]"),
                context,
            )?),
            places: Some(Box::new(compile_scalar_expression(
                places,
                format!("{path}.arguments[1]"),
                context,
            )?)),
        }),
        _ => Err(unsupported(
            format!("{path}.arguments"),
            "round() requires exactly one or two arguments",
        )),
    }
}

fn compile_sqrt_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Sqrt {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "sqrt", context,
        )?),
    })
}

fn compile_sign_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Sign {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "sign", context,
        )?),
    })
}

fn compile_exp_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Exp {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "exp", context,
        )?),
    })
}

fn compile_log_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Log {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "log", context,
        )?),
    })
}

fn compile_log10_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Log10 {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "log10", context,
        )?),
    })
}

fn compile_pi_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    compile_zero_scalar_function_arguments(function, path, "pi")?;
    Ok(ScalarExpression::Literal(Literal::Float(
        ordered_float::OrderedFloat(std::f64::consts::PI),
    )))
}

fn compile_e_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    compile_zero_scalar_function_arguments(function, path, "e")?;
    Ok(ScalarExpression::Literal(Literal::Float(
        ordered_float::OrderedFloat(std::f64::consts::E),
    )))
}

fn compile_sin_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Sin {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "sin", context,
        )?),
    })
}

fn compile_cos_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Cos {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "cos", context,
        )?),
    })
}

fn compile_tan_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Tan {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "tan", context,
        )?),
    })
}

fn compile_cot_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Cot {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "cot", context,
        )?),
    })
}

fn compile_asin_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Asin {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "asin", context,
        )?),
    })
}

fn compile_acos_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Acos {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "acos", context,
        )?),
    })
}

fn compile_atan_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Atan {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "atan", context,
        )?),
    })
}

fn compile_atan2_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (y, x) = compile_two_scalar_function_arguments(function, path, "atan2", context)?;
    Ok(ScalarExpression::Atan2 {
        y: Box::new(y),
        x: Box::new(x),
    })
}

fn compile_degrees_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Degrees {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "degrees", context,
        )?),
    })
}

fn compile_radians_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Radians {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "radians", context,
        )?),
    })
}

fn compile_haversin_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(haversin_expression(
        compile_single_scalar_function_argument(function, path, "haversin", context)?,
    ))
}

fn haversin_expression(expression: ScalarExpression) -> ScalarExpression {
    ScalarExpression::Arithmetic {
        operator: ArithmeticOperator::Divide,
        left: Box::new(ScalarExpression::Arithmetic {
            operator: ArithmeticOperator::Subtract,
            left: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
            right: Box::new(ScalarExpression::Cos {
                expression: Box::new(expression),
            }),
        }),
        right: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
    }
}

fn compile_zero_scalar_function_arguments(
    function: &FunctionInvocation,
    path: impl Into<String>,
    function_name: &str,
) -> Result<(), CoreError> {
    let path = path.into();
    if function.arguments.is_empty() {
        return Ok(());
    }
    Err(unsupported(
        format!("{path}.arguments"),
        format!("{function_name}() requires exactly zero arguments"),
    ))
}

fn compile_single_scalar_function_argument(
    function: &FunctionInvocation,
    path: impl Into<String>,
    function_name: &str,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!("{function_name}() requires exactly one argument"),
        ));
    };
    compile_scalar_expression(argument, format!("{path}.arguments[0]"), context)
}

fn single_segment_function_name(function: &FunctionInvocation) -> Option<&str> {
    match function.name.as_slice() {
        [name] => Some(name.name.as_str()),
        _ => None,
    }
}

fn compile_two_scalar_function_arguments(
    function: &FunctionInvocation,
    path: impl Into<String>,
    function_name: &str,
    context: &CypherCompileContext,
) -> Result<(ScalarExpression, ScalarExpression), CoreError> {
    let path = path.into();
    let [left, right] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!("{function_name}() requires exactly two arguments"),
        ));
    };
    Ok((
        compile_scalar_expression(left, format!("{path}.arguments[0]"), context)?,
        compile_scalar_expression(right, format!("{path}.arguments[1]"), context)?,
    ))
}

fn compile_scalar_function_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let expression = if is_coalesce_function(function) {
        compile_coalesce_scalar_expression(function, path.clone(), context)?
    } else if is_null_if_function(function) {
        compile_null_if_scalar_expression(function, path.clone(), context)?
    } else if is_to_string_function(function) {
        compile_to_string_scalar_expression(function, path.clone(), context)?
    } else if is_to_integer_function(function) {
        compile_to_integer_scalar_expression(function, path.clone(), context)?
    } else if is_to_float_function(function) {
        compile_to_float_scalar_expression(function, path.clone(), context)?
    } else if is_to_boolean_function(function) {
        compile_to_boolean_scalar_expression(function, path.clone(), context)?
    } else if is_to_string_or_null_function(function) {
        compile_to_string_or_null_scalar_expression(function, path.clone(), context)?
    } else if is_to_integer_or_null_function(function) {
        compile_to_integer_or_null_scalar_expression(function, path.clone(), context)?
    } else if is_to_float_or_null_function(function) {
        compile_to_float_or_null_scalar_expression(function, path.clone(), context)?
    } else if is_to_boolean_or_null_function(function) {
        compile_to_boolean_or_null_scalar_expression(function, path.clone(), context)?
    } else if is_to_lower_function(function) {
        compile_to_lower_scalar_expression(function, path.clone(), context)?
    } else if is_to_upper_function(function) {
        compile_to_upper_scalar_expression(function, path.clone(), context)?
    } else if is_trim_function(function) {
        compile_trim_scalar_expression(function, path.clone(), context)?
    } else if is_ltrim_function(function) {
        compile_ltrim_scalar_expression(function, path.clone(), context)?
    } else if is_rtrim_function(function) {
        compile_rtrim_scalar_expression(function, path.clone(), context)?
    } else if is_replace_function(function) {
        compile_replace_scalar_expression(function, path.clone(), context)?
    } else if is_character_length_function(function) {
        compile_character_length_scalar_expression(function, path.clone(), context)?
    } else if is_substring_function(function) {
        compile_substring_scalar_expression(function, path.clone(), context)?
    } else if is_left_function(function) {
        compile_left_scalar_expression(function, path.clone(), context)?
    } else if is_right_function(function) {
        compile_right_scalar_expression(function, path.clone(), context)?
    } else if is_reverse_function(function) {
        compile_reverse_scalar_expression(function, path.clone(), context)?
    } else if is_abs_function(function) {
        compile_abs_scalar_expression(function, path.clone(), context)?
    } else if is_ceil_function(function) {
        compile_ceil_scalar_expression(function, path.clone(), context)?
    } else if is_floor_function(function) {
        compile_floor_scalar_expression(function, path.clone(), context)?
    } else if is_round_function(function) {
        compile_round_scalar_expression(function, path.clone(), context)?
    } else if is_sqrt_function(function) {
        compile_sqrt_scalar_expression(function, path.clone(), context)?
    } else if is_sign_function(function) {
        compile_sign_scalar_expression(function, path.clone(), context)?
    } else if is_exp_function(function) {
        compile_exp_scalar_expression(function, path.clone(), context)?
    } else if is_log_function(function) {
        compile_log_scalar_expression(function, path.clone(), context)?
    } else if is_log10_function(function) {
        compile_log10_scalar_expression(function, path.clone(), context)?
    } else if is_pi_function(function) {
        compile_pi_scalar_expression(function, path.clone())?
    } else if is_e_function(function) {
        compile_e_scalar_expression(function, path.clone())?
    } else if is_sin_function(function) {
        compile_sin_scalar_expression(function, path.clone(), context)?
    } else if is_cos_function(function) {
        compile_cos_scalar_expression(function, path.clone(), context)?
    } else if is_tan_function(function) {
        compile_tan_scalar_expression(function, path.clone(), context)?
    } else if is_cot_function(function) {
        compile_cot_scalar_expression(function, path.clone(), context)?
    } else if is_asin_function(function) {
        compile_asin_scalar_expression(function, path.clone(), context)?
    } else if is_acos_function(function) {
        compile_acos_scalar_expression(function, path.clone(), context)?
    } else if is_atan_function(function) {
        compile_atan_scalar_expression(function, path.clone(), context)?
    } else if is_atan2_function(function) {
        compile_atan2_scalar_expression(function, path.clone(), context)?
    } else if is_degrees_function(function) {
        compile_degrees_scalar_expression(function, path.clone(), context)?
    } else if is_radians_function(function) {
        compile_radians_scalar_expression(function, path.clone(), context)?
    } else if is_haversin_function(function) {
        compile_haversin_scalar_expression(function, path, context)?
    } else {
        return Ok(None);
    };
    Ok(Some(expression))
}

fn compile_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_scalar_expression(inner, path, context),
        Expression::PropertyLookup { .. } => Ok(ScalarExpression::Property(compile_property_ref(
            expression, path,
        )?)),
        expression if is_literal_expression(expression) => Ok(ScalarExpression::Literal(
            compile_literal(expression, path, context)?,
        )),
        Expression::BinaryOp { op, lhs, rhs, .. } => Ok(ScalarExpression::Arithmetic {
            operator: compile_arithmetic_operator(*op, format!("{path}.operator"))?,
            left: Box::new(compile_scalar_expression(
                lhs,
                format!("{path}.lhs"),
                context,
            )?),
            right: Box::new(compile_scalar_expression(
                rhs,
                format!("{path}.rhs"),
                context,
            )?),
        }),
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            operand,
            ..
        } => Ok(ScalarExpression::Negate {
            expression: Box::new(compile_scalar_expression(
                operand,
                format!("{path}.operand"),
                context,
            )?),
        }),
        Expression::Case(case) => compile_case_scalar_expression(case, path, context),
        Expression::FunctionCall(function) => {
            compile_scalar_function_expression(function, path.clone(), context)?.ok_or_else(|| {
                unsupported(
                    path,
                    format!(
                        "scalar function '{}' is not supported here",
                        qualified_function_name(function)
                    ),
                )
            })
        }
        _ => Err(unsupported(
            path,
            "scalar expressions must be variable.property expressions, scalar literals, scalar parameters, arithmetic expressions, unary negation, nested coalesce(), nullIf(), toString(), toInteger(), toFloat(), toBoolean(), nullable scalar casts, toLower()/lower(), toUpper()/upper(), trim()/btrim(), lTrim(), rTrim(), replace(), size(), char_length(), character_length(), substring(), left(), right(), reverse(), abs(), ceil(), floor(), round(), sqrt(), sign(), exp(), log(), log10(), pi(), e(), sin(), cos(), tan(), cot(), asin(), acos(), atan(), atan2(), degrees(), radians(), or haversin() expressions",
        )),
    }
}

enum ProjectionLiteral {
    Scalar(Literal),
    List(Vec<Literal>),
}

fn compile_projection_literal(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ProjectionLiteral, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_projection_literal(inner, path, context),
        Expression::Literal(CypherLiteral::List(list)) => {
            let literals = list
                .elements
                .iter()
                .enumerate()
                .map(|(index, expression)| {
                    compile_literal(expression, format!("{path}[{index}]"), context)
                })
                .collect::<Result<Vec<_>, _>>()?;
            validate_literal_list_projection(&literals, path)?;
            Ok(ProjectionLiteral::List(literals))
        }
        Expression::Parameter(parameter) => {
            match context.parameter_value(parameter, path.clone())? {
                CypherParameterValue::Literal(value) => {
                    Ok(ProjectionLiteral::Scalar(value.clone()))
                }
                CypherParameterValue::List(values) => {
                    validate_literal_list_projection(values, path)?;
                    Ok(ProjectionLiteral::List(values.clone()))
                }
            }
        }
        _ => compile_literal(expression, path, context).map(ProjectionLiteral::Scalar),
    }
}

fn compile_id_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let variable = compile_single_variable_function_argument(
        function,
        format!("{path}.expression.arguments"),
        "id() supports exactly one graph variable argument",
        context,
    )?;
    if !plan_uses_variable(plan, &variable) {
        return Err(unsupported(
            format!("{path}.expression.arguments[0]"),
            format!("id() argument '{variable}' is not a bound graph variable"),
        ));
    }
    Ok(Projection::Key {
        variable,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "id".to_string(), variable_name),
    })
}

fn compile_element_id_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let variable =
        compile_element_id_variable(function, format!("{path}.expression"), plan, context)?;
    Ok(Projection::ElementId {
        variable,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "elementId".to_string(), variable_name),
    })
}

fn compile_type_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let variable = compile_single_variable_function_argument(
        function,
        format!("{path}.expression.arguments"),
        "type() supports exactly one relationship variable argument",
        context,
    )?;
    let relationship = plan
        .relationships
        .iter()
        .find(|relationship| relationship.variable.as_deref() == Some(variable.as_str()))
        .ok_or_else(|| {
            unsupported(
                format!("{path}.expression.arguments[0]"),
                format!("type() argument '{variable}' is not a named relationship variable"),
            )
        })?;
    Ok(Projection::RelationshipType {
        variable,
        relationship_type: relationship.relationship_type.clone(),
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "type".to_string(), variable_name),
    })
}

fn compile_labels_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let (variable, label) = compile_node_function_target(
        function,
        format!("{path}.expression.arguments"),
        "labels() supports exactly one node variable argument",
        plan,
        context,
    )?;
    Ok(Projection::NodeLabels {
        variable,
        label,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "labels".to_string(), variable_name),
    })
}

fn compile_labels_order_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    let (variable, label) = compile_node_function_target(
        function,
        format!("{path}.arguments"),
        "labels() supports exactly one node variable argument",
        plan,
        context,
    )?;
    Ok(OrderExpression::NodeLabels { variable, label })
}

fn compile_keys_order_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    let variable = compile_single_variable_function_argument(
        function,
        format!("{path}.arguments"),
        "keys() supports exactly one graph variable argument",
        context,
    )?;
    if !plan_uses_variable(plan, &variable) {
        return Err(unsupported(
            format!("{path}.arguments[0]"),
            format!("keys() argument '{variable}' is not a bound graph variable"),
        ));
    }
    Ok(OrderExpression::PropertyKeys { variable })
}

fn compile_arithmetic_order_expression(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    compile_scalar_expression(expression, path, context).map(OrderExpression::Scalar)
}

fn compile_case_order_expression(
    case: &CaseExpression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    compile_case_scalar_expression_with_plan(case, path, plan, context).map(OrderExpression::Scalar)
}

fn compile_optional_boolean_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    if is_boolean_scalar_expression(expression) {
        return compile_boolean_scalar_expression(expression, path, plan, context).map(Some);
    }
    Ok(None)
}

fn compile_boolean_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    compile_predicate_expression(expression, path, plan, context)
        .map(|predicate| ScalarExpression::Predicate(Box::new(predicate)))
}

fn is_boolean_scalar_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_boolean_scalar_expression(inner),
        Expression::BinaryOp {
            op: CypherBinaryOperator::And | CypherBinaryOperator::Or | CypherBinaryOperator::Xor,
            ..
        }
        | Expression::UnaryOp {
            op: UnaryOperator::Not,
            ..
        }
        | Expression::Comparison { .. }
        | Expression::In { .. }
        | Expression::IsNull { .. }
        | Expression::NodeLabels { .. } => true,
        Expression::FunctionCall(function) => {
            is_exists_function(function) || is_empty_function(function)
        }
        _ => false,
    }
}

fn compile_optional_predicate_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_predicate_scalar_expression(inner, path, context)
        }
        Expression::BinaryOp { .. } => {
            Ok(Some(compile_scalar_expression(expression, path, context)?))
        }
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            operand,
            ..
        } if !is_literal_expression(operand) => {
            Ok(Some(compile_scalar_expression(expression, path, context)?))
        }
        Expression::Case(case) => Ok(Some(compile_case_scalar_expression(case, path, context)?)),
        Expression::FunctionCall(function) => {
            compile_scalar_function_expression(function, path, context)
        }
        _ => Ok(None),
    }
}

fn compile_scalar_predicate_rhs(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarPredicateRhs, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_scalar_predicate_rhs(inner, path, context),
        Expression::BinaryOp { .. }
        | Expression::UnaryOp {
            op: UnaryOperator::Negate,
            ..
        } => Ok(ScalarPredicateRhs::Expression(compile_scalar_expression(
            expression, path, context,
        )?)),
        Expression::Case(case) => Ok(ScalarPredicateRhs::Expression(
            compile_case_scalar_expression(case, path, context)?,
        )),
        Expression::FunctionCall(function) => {
            match compile_scalar_function_expression(function, path.clone(), context)? {
                Some(expression) => Ok(ScalarPredicateRhs::Expression(expression)),
                None => Err(unsupported(
                    path,
                    "scalar predicates support variable.property expressions, scalar literals, scalar parameters, arithmetic expressions, unary negation, nested coalesce(), nullIf(), toString(), toInteger(), toFloat(), toBoolean(), nullable scalar casts, toLower()/lower(), toUpper()/upper(), trim()/btrim(), lTrim(), rTrim(), replace(), size(), char_length(), character_length(), substring(), left(), right(), reverse(), abs(), ceil(), floor(), round(), sqrt(), sign(), exp(), log(), log10(), pi(), e(), sin(), cos(), tan(), cot(), asin(), acos(), atan(), atan2(), degrees(), radians(), or haversin() expressions",
                )),
            }
        }
        Expression::PropertyLookup { .. } => Ok(ScalarPredicateRhs::Expression(
            ScalarExpression::Property(compile_property_ref(expression, path)?),
        )),
        expression if is_literal_expression(expression) => Ok(ScalarPredicateRhs::Expression(
            ScalarExpression::Literal(compile_literal(expression, path, context)?),
        )),
        _ => Err(unsupported(
            path,
            "scalar predicates support variable.property expressions, scalar literals, scalar parameters, arithmetic expressions, unary negation, nested coalesce(), nullIf(), toString(), toInteger(), toFloat(), toBoolean(), nullable scalar casts, toLower()/lower(), toUpper()/upper(), trim()/btrim(), lTrim(), rTrim(), replace(), size(), char_length(), character_length(), substring(), left(), right(), reverse(), abs(), ceil(), floor(), round(), sqrt(), sign(), exp(), log(), log10(), pi(), e(), sin(), cos(), tan(), cot(), asin(), acos(), atan(), atan2(), degrees(), radians(), or haversin() expressions",
        )),
    }
}

fn compile_arithmetic_operator(
    operator: CypherBinaryOperator,
    path: impl Into<String>,
) -> Result<ArithmeticOperator, CoreError> {
    match operator {
        CypherBinaryOperator::Add => Ok(ArithmeticOperator::Add),
        CypherBinaryOperator::Subtract => Ok(ArithmeticOperator::Subtract),
        CypherBinaryOperator::Multiply => Ok(ArithmeticOperator::Multiply),
        CypherBinaryOperator::Divide => Ok(ArithmeticOperator::Divide),
        CypherBinaryOperator::Modulo => Ok(ArithmeticOperator::Modulo),
        CypherBinaryOperator::Power => Ok(ArithmeticOperator::Power),
        CypherBinaryOperator::And | CypherBinaryOperator::Or | CypherBinaryOperator::Xor => {
            Err(unsupported(
                path,
                "boolean operators are not scalar arithmetic expressions",
            ))
        }
    }
}

fn compile_case_scalar_expression(
    case: &CaseExpression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    compile_case_scalar_expression_in_mode(
        case,
        path,
        PredicateCompileMode::CaseWhen { plan: None },
        context,
    )
}

fn compile_case_scalar_expression_with_plan(
    case: &CaseExpression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    compile_case_scalar_expression_in_mode(
        case,
        path,
        PredicateCompileMode::CaseWhen { plan: Some(plan) },
        context,
    )
}

fn compile_case_scalar_expression_in_mode(
    case: &CaseExpression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    if case.alternatives.is_empty() {
        return Err(unsupported(
            format!("{path}.alternatives"),
            "CASE expressions require at least one WHEN/THEN alternative",
        ));
    }

    let alternatives = case
        .alternatives
        .iter()
        .enumerate()
        .map(|(index, alternative)| {
            let when = if let Some(scrutinee) = &case.scrutinee {
                compile_binary_comparison(
                    scrutinee,
                    CypherComparisonOperator::Eq,
                    &alternative.when,
                    format!("{path}.alternatives[{index}].when"),
                    mode,
                    context,
                )?
            } else {
                compile_predicate_expression_in_mode(
                    &alternative.when,
                    format!("{path}.alternatives[{index}].when"),
                    mode,
                    context,
                )?
            };
            Ok(ScalarCaseAlternative {
                when,
                then: compile_scalar_expression(
                    &alternative.then,
                    format!("{path}.alternatives[{index}].then"),
                    context,
                )?,
            })
        })
        .collect::<Result<Vec<_>, CoreError>>()?;
    let else_expression = case
        .default
        .as_ref()
        .map(|expression| {
            compile_scalar_expression(expression, format!("{path}.default"), context).map(Box::new)
        })
        .transpose()?;

    Ok(ScalarExpression::Case {
        alternatives,
        else_expression,
    })
}

fn compile_keys_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let variable = compile_single_variable_function_argument(
        function,
        format!("{path}.expression.arguments"),
        "keys() supports exactly one graph variable argument",
        context,
    )?;
    Ok(Projection::PropertyKeys {
        variable,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "keys".to_string(), variable_name),
    })
}

fn compile_node_function_target(
    function: &FunctionInvocation,
    path: impl Into<String>,
    message: &'static str,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<(String, String), CoreError> {
    let path = path.into();
    let variable =
        compile_single_variable_function_argument(function, path.clone(), message, context)?;
    let node = plan
        .nodes
        .iter()
        .find(|node| node.variable == variable)
        .ok_or_else(|| {
            unsupported(
                format!("{path}[0]"),
                format!("labels() argument '{variable}' is not a node variable"),
            )
        })?;
    Ok((variable, node.label.clone()))
}

fn compile_single_variable_function_argument(
    function: &FunctionInvocation,
    path: impl Into<String>,
    message: &'static str,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    match function.arguments.as_slice() {
        [Expression::Parenthesized(inner)] => match inner.as_ref() {
            Expression::Variable(variable) => Ok(variable_name(variable)),
            _ => Err(unsupported(format!("{path}[0]"), message)),
        },
        [Expression::Variable(variable)] => Ok(variable_name(variable)),
        [] => context
            .variable_function_argument(function)
            .map(str::to_string)
            .ok_or_else(|| unsupported(path, message)),
        _ => Err(unsupported(path, message)),
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
            let variable = context.variable_function_argument(function).ok_or_else(|| {
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

fn collect_variable_function_arguments(cypher: &str) -> BTreeMap<(usize, usize), String> {
    // decypher's high-level AST currently drops variable-only function
    // arguments such as count(n), id(n), and type(r); the lossless CST keeps
    // them by span.
    let parse = decypher::parse_cst(cypher);
    let tree = parse.tree();
    tree.syntax()
        .descendants()
        .filter(|node| node.kind() == SyntaxKind::FUNCTION_INVOCATION)
        .filter_map(|node| variable_function_argument_from_cst(&node))
        .collect()
}

fn variable_function_argument_from_cst(node: &SyntaxNode) -> Option<((usize, usize), String)> {
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
    } else if name.name.eq_ignore_ascii_case("collect") {
        Some(AggregateFunction::Collect)
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
        AggregateFunction::Collect => "collect",
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

fn is_empty_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("isEmpty")
    )
}

fn is_id_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("id")
    )
}

fn is_element_id_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("elementId")
    )
}

fn is_type_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("type")
    )
}

fn is_labels_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("labels")
    )
}

fn is_keys_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("keys")
    )
}

fn is_coalesce_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("coalesce")
    )
}

fn is_null_if_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("nullIf")
    )
}

fn is_to_string_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toString")
    )
}

fn is_to_integer_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toInteger")
    )
}

fn is_to_float_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toFloat")
    )
}

fn is_to_boolean_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toBoolean")
    )
}

fn is_to_string_or_null_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toStringOrNull")
    )
}

fn is_to_integer_or_null_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toIntegerOrNull")
    )
}

fn is_to_float_or_null_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toFloatOrNull")
    )
}

fn is_to_boolean_or_null_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toBooleanOrNull")
    )
}

fn is_to_lower_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toLower")
            || name.name.eq_ignore_ascii_case("lower")
    )
}

fn is_to_upper_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toUpper")
            || name.name.eq_ignore_ascii_case("upper")
    )
}

fn is_trim_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("trim")
            || name.name.eq_ignore_ascii_case("btrim")
    )
}

fn is_ltrim_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("lTrim")
    )
}

fn is_rtrim_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("rTrim")
    )
}

fn is_replace_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("replace")
    )
}

fn is_character_length_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("size")
            || name.name.eq_ignore_ascii_case("char_length")
            || name.name.eq_ignore_ascii_case("character_length")
    )
}

fn is_substring_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("substring")
    )
}

fn is_left_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("left")
    )
}

fn is_right_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("right")
    )
}

fn is_reverse_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("reverse")
    )
}

fn is_abs_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("abs")
    )
}

fn is_ceil_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("ceil")
    )
}

fn is_floor_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("floor")
    )
}

fn is_round_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("round")
    )
}

fn is_sqrt_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("sqrt")
    )
}

fn is_sign_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("sign")
    )
}

fn is_exp_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("exp")
    )
}

fn is_log_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("log")
    )
}

fn is_log10_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("log10")
    )
}

fn is_pi_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("pi")
    )
}

fn is_e_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("e")
    )
}

fn is_sin_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("sin")
    )
}

fn is_cos_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("cos")
    )
}

fn is_tan_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("tan")
    )
}

fn is_cot_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("cot")
    )
}

fn is_asin_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("asin")
    )
}

fn is_acos_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("acos")
    )
}

fn is_atan_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("atan")
    )
}

fn is_atan2_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("atan2")
    )
}

fn is_degrees_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("degrees")
    )
}

fn is_radians_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("radians")
    )
}

fn is_haversin_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("haversin")
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
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    compile_predicate_expression_in_mode(
        expression,
        path,
        PredicateCompileMode::Graph { plan },
        context,
    )
}

fn compile_predicate_expression_in_mode(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_predicate_expression_in_mode(inner, path, mode, context)
        }
        Expression::BinaryOp {
            op: CypherBinaryOperator::Xor,
            lhs,
            rhs,
            ..
        }
        | Expression::BinaryOp {
            op: CypherBinaryOperator::And | CypherBinaryOperator::Or,
            lhs,
            rhs,
            ..
        } => compile_binary_predicate_expression(expression, lhs, rhs, &path, mode, context),
        Expression::UnaryOp {
            op: UnaryOperator::Not,
            operand,
            ..
        } => Ok(PredicateExpression::Not {
            expression: Box::new(compile_predicate_expression_in_mode(
                operand,
                format!("{path}.operand"),
                mode,
                context,
            )?),
        }),
        Expression::Comparison { lhs, operators, .. } => {
            compile_comparison_expression(lhs, operators.as_slice(), path, mode, context)
        }
        Expression::In { lhs, rhs, .. } => compile_in_predicate(lhs, rhs, path, mode, context),
        Expression::NodeLabels { base, labels, .. } => match mode.graph_plan() {
            Some(plan) => compile_graph_label_predicate(base, labels, path, plan),
            None => Err(unsupported(
                path,
                "CASE WHEN label predicates are not supported yet",
            )),
        },
        Expression::Literal(CypherLiteral::Boolean(value)) => {
            Ok(PredicateExpression::Boolean(*value))
        }
        Expression::IsNull {
            operand, negated, ..
        } => compile_null_predicate(operand, *negated, path, mode, context),
        Expression::FunctionCall(function) if is_exists_function(function) => Ok(
            PredicateExpression::Comparison(compile_exists_predicate(function, path)?),
        ),
        Expression::FunctionCall(function) if is_empty_function(function) => {
            Ok(PredicateExpression::ScalarComparison(
                compile_is_empty_predicate(function, path, context)?,
            ))
        }
        Expression::PropertyLookup { .. } => {
            Ok(PredicateExpression::Comparison(PropertyPredicate {
                property: compile_property_ref(expression, path)?,
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::Boolean(true)),
            }))
        }
        _ => Err(unsupported(path, mode.unsupported_predicate_message())),
    }
}

fn compile_binary_predicate_expression(
    expression: &Expression,
    lhs: &Expression,
    rhs: &Expression,
    path: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let Expression::BinaryOp { op, .. } = expression else {
        unreachable!("binary predicate helper called with non-binary expression");
    };
    let left = Box::new(compile_predicate_expression_in_mode(
        lhs,
        format!("{path}.lhs"),
        mode,
        context,
    )?);
    let right = Box::new(compile_predicate_expression_in_mode(
        rhs,
        format!("{path}.rhs"),
        mode,
        context,
    )?);
    match op {
        CypherBinaryOperator::And => Ok(PredicateExpression::And { left, right }),
        CypherBinaryOperator::Or => Ok(PredicateExpression::Or { left, right }),
        CypherBinaryOperator::Xor => Ok(PredicateExpression::Xor { left, right }),
        _ => unreachable!("non-boolean operator reached binary predicate helper"),
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

fn compile_is_empty_predicate(
    function: &FunctionInvocation,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarPredicate, CoreError> {
    let path = path.into();
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "isEmpty() supports exactly one scalar string argument",
        ));
    };
    Ok(ScalarPredicate {
        lhs: ScalarExpression::CharacterLength {
            expression: Box::new(compile_scalar_expression(
                argument,
                format!("{path}.arguments[0]"),
                context,
            )?),
        },
        operator: ComparisonOperator::Equal,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
    })
}

fn compile_projection_predicate_expression(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ProjectionPredicateExpression, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_projection_predicate_expression(inner, path, context)
        }
        Expression::BinaryOp {
            op: CypherBinaryOperator::Xor,
            lhs,
            rhs,
            ..
        }
        | Expression::BinaryOp {
            op: CypherBinaryOperator::And | CypherBinaryOperator::Or,
            lhs,
            rhs,
            ..
        } => compile_binary_projection_predicate_expression(expression, lhs, rhs, &path, context),
        Expression::UnaryOp {
            op: UnaryOperator::Not,
            operand,
            ..
        } => Ok(ProjectionPredicateExpression::Not {
            expression: Box::new(compile_projection_predicate_expression(
                operand,
                format!("{path}.operand"),
                context,
            )?),
        }),
        Expression::Comparison { lhs, operators, .. } => {
            compile_projection_comparison_expression(lhs, operators.as_slice(), path, context)
        }
        Expression::In { lhs, rhs, .. } => Ok(ProjectionPredicateExpression::Comparison(
            compile_projection_in_predicate(lhs, rhs, path, context)?,
        )),
        Expression::Literal(CypherLiteral::Boolean(value)) => {
            Ok(ProjectionPredicateExpression::Boolean(*value))
        }
        Expression::IsNull {
            operand, negated, ..
        } => Ok(ProjectionPredicateExpression::Comparison(
            ProjectionPredicate {
                alias: compile_projection_alias_ref(operand, format!("{path}.operand"))?,
                operator: if *negated {
                    ComparisonOperator::NotEqual
                } else {
                    ComparisonOperator::Equal
                },
                rhs: ProjectionPredicateRhs::Literal(Literal::Null),
            },
        )),
        Expression::Variable(variable) => Ok(ProjectionPredicateExpression::Comparison(
            ProjectionPredicate {
                alias: variable_name(variable),
                operator: ComparisonOperator::Equal,
                rhs: ProjectionPredicateRhs::Literal(Literal::Boolean(true)),
            },
        )),
        _ => Err(unsupported(
            path,
            "WITH WHERE only supports projected alias comparisons combined with AND, OR, XOR, and NOT",
        )),
    }
}

fn compile_binary_projection_predicate_expression(
    expression: &Expression,
    lhs: &Expression,
    rhs: &Expression,
    path: &str,
    context: &CypherCompileContext,
) -> Result<ProjectionPredicateExpression, CoreError> {
    let Expression::BinaryOp { op, .. } = expression else {
        unreachable!("binary projection predicate helper called with non-binary expression");
    };
    let left = Box::new(compile_projection_predicate_expression(
        lhs,
        format!("{path}.lhs"),
        context,
    )?);
    let right = Box::new(compile_projection_predicate_expression(
        rhs,
        format!("{path}.rhs"),
        context,
    )?);
    match op {
        CypherBinaryOperator::And => Ok(ProjectionPredicateExpression::And { left, right }),
        CypherBinaryOperator::Or => Ok(ProjectionPredicateExpression::Or { left, right }),
        CypherBinaryOperator::Xor => Ok(ProjectionPredicateExpression::Xor { left, right }),
        _ => unreachable!("non-boolean operator reached binary projection predicate helper"),
    }
}

fn compile_projection_comparison_expression(
    lhs: &Expression,
    operators: &[(CypherComparisonOperator, Box<Expression>)],
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ProjectionPredicateExpression, CoreError> {
    let path = path.into();
    if operators.is_empty() {
        return Err(unsupported(path, "comparison must include an operator"));
    }

    let (prefix, mut current_lhs) =
        compile_projection_comparison_prefix(lhs, format!("{path}.lhs"), context)?;
    let mut expression = prefix;
    for (index, (operator, rhs)) in operators.iter().enumerate() {
        let predicate = compile_binary_projection_comparison(
            current_lhs,
            *operator,
            rhs,
            format!("{path}.operators[{index}]"),
            context,
        )?;
        let next = ProjectionPredicateExpression::Comparison(predicate);
        expression = Some(append_projection_expression_conjunct(expression, next));
        current_lhs = rhs;
    }

    expression.ok_or_else(|| CoreError::internal("projection comparison expression was empty"))
}

fn compile_projection_comparison_prefix<'a>(
    expression: &'a Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<(Option<ProjectionPredicateExpression>, &'a Expression), CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_projection_comparison_prefix(inner, path, context)
        }
        Expression::Comparison { lhs, operators, .. } => Ok((
            Some(compile_projection_comparison_expression(
                lhs,
                operators.as_slice(),
                path,
                context,
            )?),
            terminal_comparison_operand(lhs, operators.as_slice()),
        )),
        _ => Ok((None, expression)),
    }
}

fn append_projection_expression_conjunct(
    expression: Option<ProjectionPredicateExpression>,
    next: ProjectionPredicateExpression,
) -> ProjectionPredicateExpression {
    match expression {
        Some(previous) => ProjectionPredicateExpression::And {
            left: Box::new(previous),
            right: Box::new(next),
        },
        None => next,
    }
}

fn compile_binary_projection_comparison(
    lhs: &Expression,
    operator: CypherComparisonOperator,
    rhs: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ProjectionPredicate, CoreError> {
    let path = path.into();
    let operator = compile_comparison_operator(operator);
    if let Some(alias) = compile_optional_projection_alias_ref(lhs) {
        return Ok(ProjectionPredicate {
            alias,
            operator,
            rhs: compile_projection_predicate_rhs(rhs, format!("{path}.rhs"), context)?,
        });
    }
    if let Some(alias) = compile_optional_projection_alias_ref(rhs) {
        return Ok(ProjectionPredicate {
            alias,
            operator: invert_comparison_operator(operator, format!("{path}.operator"))?,
            rhs: ProjectionPredicateRhs::Literal(compile_literal(
                lhs,
                format!("{path}.lhs"),
                context,
            )?),
        });
    }

    Err(unsupported(
        path,
        "WITH WHERE comparisons must include at least one projected alias operand",
    ))
}

fn compile_projection_in_predicate(
    lhs: &Expression,
    rhs: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ProjectionPredicate, CoreError> {
    let path = path.into();
    Ok(ProjectionPredicate {
        alias: compile_projection_alias_ref(lhs, format!("{path}.lhs"))?,
        operator: ComparisonOperator::In,
        rhs: ProjectionPredicateRhs::List(compile_literal_list(
            rhs,
            format!("{path}.rhs"),
            context,
        )?),
    })
}

fn compile_projection_predicate_rhs(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ProjectionPredicateRhs, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_projection_predicate_rhs(inner, path, context),
        Expression::Variable(variable) => {
            Ok(ProjectionPredicateRhs::Alias(variable_name(variable)))
        }
        _ => Ok(ProjectionPredicateRhs::Literal(compile_literal(
            expression, path, context,
        )?)),
    }
}

fn compile_projection_alias_ref(
    expression: &Expression,
    path: impl Into<String>,
) -> Result<String, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_projection_alias_ref(inner, path),
        Expression::Variable(variable) => Ok(variable_name(variable)),
        _ => Err(unsupported(
            path,
            "only projected alias expressions are supported here",
        )),
    }
}

fn compile_optional_projection_alias_ref(expression: &Expression) -> Option<String> {
    match expression {
        Expression::Parenthesized(inner) => compile_optional_projection_alias_ref(inner),
        Expression::Variable(variable) => Some(variable_name(variable)),
        _ => None,
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
        PredicateExpression::Boolean(_)
        | PredicateExpression::KeyComparison(_)
        | PredicateExpression::ElementIdComparison(_)
        | PredicateExpression::Presence(_)
        | PredicateExpression::PropertyKeyMembership(_)
        | PredicateExpression::ScalarComparison(_)
        | PredicateExpression::Or { .. }
        | PredicateExpression::Xor { .. }
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
        | PredicateExpression::KeyComparison(_)
        | PredicateExpression::ElementIdComparison(_)
        | PredicateExpression::Presence(_)
        | PredicateExpression::PropertyKeyMembership(_)
        | PredicateExpression::ScalarComparison(_)
        | PredicateExpression::Or { .. }
        | PredicateExpression::Xor { .. }
        | PredicateExpression::Not { .. } => {
            unreachable!("non-conjunctive predicate expression reached conjunctive appender")
        }
    }
}

fn compile_comparison_expression(
    lhs: &Expression,
    operators: &[(CypherComparisonOperator, Box<Expression>)],
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    if operators.is_empty() {
        return Err(unsupported(path, "comparison must include an operator"));
    }

    let (prefix, mut current_lhs) =
        compile_comparison_prefix(lhs, format!("{path}.lhs"), mode, context)?;
    let mut expression = prefix;
    for (index, (operator, rhs)) in operators.iter().enumerate() {
        let next = compile_binary_comparison(
            current_lhs,
            *operator,
            rhs,
            format!("{path}.operators[{index}]"),
            mode,
            context,
        )?;
        expression = Some(append_expression_conjunct(expression, next));
        current_lhs = rhs;
    }

    expression.ok_or_else(|| CoreError::internal("comparison expression was empty"))
}

fn compile_comparison_prefix<'a>(
    expression: &'a Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<(Option<PredicateExpression>, &'a Expression), CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_comparison_prefix(inner, path, mode, context),
        Expression::Comparison { lhs, operators, .. } => Ok((
            Some(compile_comparison_expression(
                lhs,
                operators.as_slice(),
                path,
                mode,
                context,
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
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let operator = compile_comparison_operator(operator);
    if let Some(property) = compile_optional_property_ref(lhs, format!("{path}.lhs"))? {
        return compile_left_property_comparison(property, operator, rhs, &path, mode, context);
    }
    if let Some(plan) = mode.graph_plan() {
        if let Some(variable) = compile_optional_id_ref(lhs, format!("{path}.lhs"), plan, context)?
        {
            return Ok(PredicateExpression::KeyComparison(KeyPredicate {
                variable,
                operator,
                rhs: compile_predicate_rhs(rhs, format!("{path}.rhs"), mode, context)?,
            }));
        }
        if let Some(variable) =
            compile_optional_element_id_ref(lhs, format!("{path}.lhs"), plan, context)?
        {
            return Ok(PredicateExpression::ElementIdComparison(
                ElementIdPredicate {
                    variable,
                    operator,
                    rhs: compile_predicate_rhs(rhs, format!("{path}.rhs"), mode, context)?,
                },
            ));
        }
    }
    if let Some(lhs) =
        compile_optional_predicate_scalar_expression(lhs, format!("{path}.lhs"), context)?
    {
        return Ok(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs,
            operator,
            rhs: compile_scalar_predicate_rhs(rhs, format!("{path}.rhs"), context)?,
        }));
    }
    if let Some(property) = compile_optional_property_ref(rhs, format!("{path}.rhs"))? {
        return Ok(PredicateExpression::Comparison(PropertyPredicate {
            property,
            operator: invert_comparison_operator(operator, format!("{path}.operator"))?,
            rhs: compile_literal_predicate_rhs(lhs, format!("{path}.lhs"), mode, context)?,
        }));
    }
    if let Some(plan) = mode.graph_plan() {
        if let Some(variable) = compile_optional_id_ref(rhs, format!("{path}.rhs"), plan, context)?
        {
            return Ok(PredicateExpression::KeyComparison(KeyPredicate {
                variable,
                operator: invert_comparison_operator(operator, format!("{path}.operator"))?,
                rhs: compile_literal_predicate_rhs(lhs, format!("{path}.lhs"), mode, context)?,
            }));
        }
        if let Some(variable) =
            compile_optional_element_id_ref(rhs, format!("{path}.rhs"), plan, context)?
        {
            return Ok(PredicateExpression::ElementIdComparison(
                ElementIdPredicate {
                    variable,
                    operator: invert_comparison_operator(operator, format!("{path}.operator"))?,
                    rhs: compile_literal_predicate_rhs(lhs, format!("{path}.lhs"), mode, context)?,
                },
            ));
        }
    }
    if let Some(rhs) =
        compile_optional_predicate_scalar_expression(rhs, format!("{path}.rhs"), context)?
    {
        return Ok(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: rhs,
            operator: invert_comparison_operator(operator, format!("{path}.operator"))?,
            rhs: compile_scalar_predicate_rhs(lhs, format!("{path}.lhs"), context)?,
        }));
    }

    if let Some(plan) = mode.static_metadata_plan()
        && (contains_type_function(lhs) || contains_type_function(rhs))
    {
        let lhs = compile_predicate_literal(lhs, format!("{path}.lhs"), plan, context)?;
        let rhs = compile_predicate_literal(rhs, format!("{path}.rhs"), plan, context)?;
        return Ok(PredicateExpression::Boolean(evaluate_literal_comparison(
            &lhs, operator, &rhs, path,
        )?));
    }
    if is_literal_expression(lhs) && is_literal_expression(rhs) {
        let lhs = compile_literal(lhs, format!("{path}.lhs"), context)?;
        let rhs = compile_literal(rhs, format!("{path}.rhs"), context)?;
        return Ok(PredicateExpression::Boolean(
            evaluate_literal_only_comparison(&lhs, operator, &rhs, path)?,
        ));
    }

    Err(unsupported(path, mode.unsupported_comparison_message()))
}

fn compile_left_property_comparison(
    property: PropertyRef,
    operator: ComparisonOperator,
    rhs: &Expression,
    path: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    if let Some(predicate) =
        compile_dynamic_string_property_predicate(&property, operator, rhs, path, context)?
    {
        return Ok(predicate);
    }
    if let Some(predicate) =
        compile_dynamic_scalar_property_predicate(&property, operator, rhs, path, context)?
    {
        return Ok(predicate);
    }
    Ok(PredicateExpression::Comparison(PropertyPredicate {
        property,
        operator,
        rhs: compile_predicate_rhs(rhs, format!("{path}.rhs"), mode, context)?,
    }))
}

fn compile_dynamic_string_property_predicate(
    property: &PropertyRef,
    operator: ComparisonOperator,
    rhs: &Expression,
    path: &str,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    if !is_string_comparison_operator(operator) || is_literal_expression(rhs) {
        return Ok(None);
    }

    let Some(rhs) =
        compile_optional_predicate_scalar_expression(rhs, format!("{path}.rhs"), context)?
    else {
        return Ok(None);
    };

    Ok(Some(PredicateExpression::ScalarComparison(
        ScalarPredicate {
            lhs: ScalarExpression::Property(property.clone()),
            operator,
            rhs: ScalarPredicateRhs::Expression(rhs),
        },
    )))
}

fn compile_dynamic_scalar_property_predicate(
    property: &PropertyRef,
    operator: ComparisonOperator,
    rhs: &Expression,
    path: &str,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    if is_string_comparison_operator(operator) || is_literal_expression(rhs) {
        return Ok(None);
    }

    let Some(rhs) =
        compile_optional_predicate_scalar_expression(rhs, format!("{path}.rhs"), context)?
    else {
        return Ok(None);
    };

    Ok(Some(PredicateExpression::ScalarComparison(
        ScalarPredicate {
            lhs: ScalarExpression::Property(property.clone()),
            operator,
            rhs: ScalarPredicateRhs::Expression(rhs),
        },
    )))
}

fn compile_in_predicate(
    lhs: &Expression,
    rhs: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    if let Some(plan) = mode.graph_plan() {
        if let Some(predicate) =
            compile_label_membership_predicate(lhs, rhs, path.clone(), plan, context)?
        {
            return Ok(predicate);
        }
        if let Some(predicate) =
            compile_property_key_membership_predicate(lhs, rhs, path.clone(), plan, context)?
        {
            return Ok(predicate);
        }
    }
    let literals = compile_literal_list(rhs, format!("{path}.rhs"), context)?;
    if let Some(property) = compile_optional_property_ref(lhs, format!("{path}.lhs"))? {
        return Ok(PredicateExpression::Comparison(PropertyPredicate {
            property,
            operator: ComparisonOperator::In,
            rhs: PredicateRhs::List(literals),
        }));
    }
    if let Some(plan) = mode.graph_plan() {
        if let Some(variable) = compile_optional_id_ref(lhs, format!("{path}.lhs"), plan, context)?
        {
            return Ok(PredicateExpression::KeyComparison(KeyPredicate {
                variable,
                operator: ComparisonOperator::In,
                rhs: PredicateRhs::List(literals),
            }));
        }
        if let Some(variable) =
            compile_optional_element_id_ref(lhs, format!("{path}.lhs"), plan, context)?
        {
            return Ok(PredicateExpression::ElementIdComparison(
                ElementIdPredicate {
                    variable,
                    operator: ComparisonOperator::In,
                    rhs: PredicateRhs::List(literals),
                },
            ));
        }
    }
    if let Some(lhs) =
        compile_optional_predicate_scalar_expression(lhs, format!("{path}.lhs"), context)?
    {
        return Ok(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs,
            operator: ComparisonOperator::In,
            rhs: ScalarPredicateRhs::List(literals),
        }));
    }
    if let Some(plan) = mode.static_metadata_plan()
        && contains_type_function(lhs)
    {
        let literal = compile_predicate_literal(lhs, format!("{path}.lhs"), plan, context)?;
        return Ok(PredicateExpression::Boolean(evaluate_literal_in_list(
            &literal, &literals, path,
        )?));
    }
    if is_literal_expression(lhs) {
        let literal = compile_literal(lhs, format!("{path}.lhs"), context)?;
        return Ok(PredicateExpression::Boolean(evaluate_literal_in_list(
            &literal, &literals, path,
        )?));
    }
    Err(unsupported(
        format!("{path}.lhs"),
        mode.unsupported_in_message(),
    ))
}

fn compile_property_key_membership_predicate(
    lhs: &Expression,
    rhs: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    let Some(variable) = compile_optional_keys_ref(rhs, format!("{path}.rhs"), plan, context)?
    else {
        return Ok(None);
    };
    let literal = compile_predicate_literal(lhs, format!("{path}.lhs"), plan, context)?;
    let Literal::String(key) = literal else {
        return Err(unsupported(
            format!("{path}.lhs"),
            "keys() membership predicates require a string literal or scalar string parameter",
        ));
    };
    Ok(Some(PredicateExpression::PropertyKeyMembership(
        PropertyKeyMembershipPredicate { variable, key },
    )))
}

fn compile_label_membership_predicate(
    lhs: &Expression,
    rhs: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    let Some((_, label)) = compile_optional_labels_ref(rhs, format!("{path}.rhs"), plan, context)?
    else {
        return Ok(None);
    };
    let literal = compile_predicate_literal(lhs, format!("{path}.lhs"), plan, context)?;
    let Literal::String(candidate) = literal else {
        return Err(unsupported(
            format!("{path}.lhs"),
            "label membership predicates require a string literal or scalar string parameter",
        ));
    };
    Ok(Some(PredicateExpression::Boolean(candidate == label)))
}

fn compile_graph_label_predicate(
    base: &Expression,
    labels: &[LabelExpression],
    path: impl Into<String>,
    plan: &GraphPlan,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let variable = compile_label_predicate_variable(base, format!("{path}.base"))?;
    let mapped_label = mapped_graph_label_for_variable(plan, &variable).ok_or_else(|| {
        unsupported(
            format!("{path}.base"),
            format!("label predicate variable '{variable}' is not a node or relationship variable"),
        )
    })?;
    if labels.is_empty() {
        return Err(unsupported(
            format!("{path}.labels"),
            "graph label predicates require at least one label or relationship type",
        ));
    }

    let matches = labels.iter().enumerate().try_fold(
        true,
        |matches, (index, label)| -> Result<bool, CoreError> {
            Ok(matches
                && evaluate_static_label_expression(
                    label,
                    mapped_label,
                    format!("{path}.labels[{index}]"),
                )?)
        },
    )?;
    Ok(PredicateExpression::Boolean(matches))
}

fn compile_label_predicate_variable(
    expression: &Expression,
    path: impl Into<String>,
) -> Result<String, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_label_predicate_variable(inner, path),
        Expression::Variable(variable) => Ok(variable_name(variable)),
        _ => Err(unsupported(
            path,
            "graph label predicates require a node or relationship variable",
        )),
    }
}

fn mapped_graph_label_for_variable<'a>(plan: &'a GraphPlan, variable: &str) -> Option<&'a str> {
    if let Some(node) = plan.nodes.iter().find(|node| node.variable == variable) {
        return Some(node.label.as_str());
    }
    plan.relationships
        .iter()
        .find(|relationship| relationship.variable.as_deref() == Some(variable))
        .map(|relationship| relationship.relationship_type.as_str())
}

fn evaluate_static_label_expression(
    expression: &LabelExpression,
    mapped_label: &str,
    path: impl Into<String>,
) -> Result<bool, CoreError> {
    let path = path.into();
    match expression {
        LabelExpression::Static(label) => Ok(label.name == mapped_label),
        LabelExpression::Dynamic { .. } => Err(unsupported(
            path,
            "dynamic label predicates are not supported yet",
        )),
        LabelExpression::Or { lhs, rhs, .. } => {
            Ok(
                evaluate_static_label_expression(lhs, mapped_label, format!("{path}.lhs"))?
                    || evaluate_static_label_expression(rhs, mapped_label, format!("{path}.rhs"))?,
            )
        }
        LabelExpression::And { lhs, rhs, .. } => {
            Ok(
                evaluate_static_label_expression(lhs, mapped_label, format!("{path}.lhs"))?
                    && evaluate_static_label_expression(rhs, mapped_label, format!("{path}.rhs"))?,
            )
        }
        LabelExpression::Not { inner, .. } => Ok(!evaluate_static_label_expression(
            inner,
            mapped_label,
            format!("{path}.inner"),
        )?),
        LabelExpression::Group { inner, .. } => {
            evaluate_static_label_expression(inner, mapped_label, path)
        }
    }
}

fn compile_optional_keys_ref(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<String>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_optional_keys_ref(inner, path, plan, context),
        Expression::FunctionCall(function) if is_keys_function(function) => {
            let variable = compile_single_variable_function_argument(
                function,
                format!("{path}.arguments"),
                "keys() supports exactly one graph variable argument",
                context,
            )?;
            if !plan_uses_variable(plan, &variable) {
                return Err(unsupported(
                    format!("{path}.arguments[0]"),
                    format!("keys() argument '{variable}' is not a bound graph variable"),
                ));
            }
            Ok(Some(variable))
        }
        _ => Ok(None),
    }
}

fn compile_optional_labels_ref(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<(String, String)>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_optional_labels_ref(inner, path, plan, context),
        Expression::FunctionCall(function) if is_labels_function(function) => {
            Ok(Some(compile_node_function_target(
                function,
                format!("{path}.arguments"),
                "labels() supports exactly one node variable argument",
                plan,
                context,
            )?))
        }
        _ => Ok(None),
    }
}

fn compile_predicate_rhs(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateRhs, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_predicate_rhs(inner, path, mode, context),
        Expression::PropertyLookup { .. } => Ok(PredicateRhs::Property(compile_property_ref(
            expression, path,
        )?)),
        Expression::FunctionCall(function) if is_id_function(function) => match mode.graph_plan() {
            Some(plan) => Ok(PredicateRhs::Key {
                variable: compile_id_variable(function, path, plan, context)?,
            }),
            None => Err(unsupported(
                path,
                "CASE WHEN property comparisons do not support id() right-hand sides yet",
            )),
        },
        Expression::FunctionCall(function) if is_element_id_function(function) => {
            match mode.graph_plan() {
                Some(plan) => Ok(PredicateRhs::ElementId {
                    variable: compile_element_id_variable(function, path, plan, context)?,
                }),
                None => Err(unsupported(
                    path,
                    "CASE WHEN property comparisons do not support elementId() right-hand sides yet",
                )),
            }
        }
        _ => Ok(PredicateRhs::Literal(compile_predicate_literal_in_mode(
            expression, path, mode, context,
        )?)),
    }
}

fn compile_literal_predicate_rhs(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateRhs, CoreError> {
    Ok(PredicateRhs::Literal(compile_predicate_literal_in_mode(
        expression, path, mode, context,
    )?))
}

fn compile_null_predicate(
    operand: &Expression,
    negated: bool,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let operator = if negated {
        ComparisonOperator::NotEqual
    } else {
        ComparisonOperator::Equal
    };
    if let Some(property) = compile_optional_property_ref(operand, format!("{path}.operand"))? {
        return Ok(PredicateExpression::Comparison(PropertyPredicate {
            property,
            operator,
            rhs: PredicateRhs::Literal(Literal::Null),
        }));
    }
    if let Some(plan) = mode.graph_plan() {
        if let Some(variable) =
            compile_optional_id_ref(operand, format!("{path}.operand"), plan, context)?
        {
            return Ok(PredicateExpression::KeyComparison(KeyPredicate {
                variable,
                operator,
                rhs: PredicateRhs::Literal(Literal::Null),
            }));
        }
        if let Some(variable) =
            compile_optional_element_id_ref(operand, format!("{path}.operand"), plan, context)?
        {
            return Ok(PredicateExpression::ElementIdComparison(
                ElementIdPredicate {
                    variable,
                    operator,
                    rhs: PredicateRhs::Literal(Literal::Null),
                },
            ));
        }
    }
    if let Some(lhs) =
        compile_optional_predicate_scalar_expression(operand, format!("{path}.operand"), context)?
    {
        return Ok(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs,
            operator,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null)),
        }));
    }
    if let Some(plan) = mode.graph_plan() {
        if let Some(variable) = compile_optional_graph_variable_ref(operand) {
            if !plan_uses_variable(plan, &variable) {
                return Err(unsupported(
                    format!("{path}.operand"),
                    format!("IS NULL argument '{variable}' is not a bound graph variable"),
                ));
            }
            return Ok(PredicateExpression::Presence(PresencePredicate {
                variable,
                operator,
            }));
        }
        if mode.graph_metadata_plan().is_some() && contains_type_function(operand) {
            return Ok(PredicateExpression::Boolean(negated));
        }
    }
    Err(unsupported(
        format!("{path}.operand"),
        mode.unsupported_null_message(),
    ))
}

fn compile_optional_graph_variable_ref(expression: &Expression) -> Option<String> {
    match expression {
        Expression::Parenthesized(inner) => compile_optional_graph_variable_ref(inner),
        Expression::Variable(variable) => Some(variable_name(variable)),
        _ => None,
    }
}

fn compile_optional_id_ref(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<String>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_optional_id_ref(inner, path, plan, context),
        Expression::FunctionCall(function) if is_id_function(function) => {
            Ok(Some(compile_id_variable(function, path, plan, context)?))
        }
        _ => Ok(None),
    }
}

fn compile_optional_element_id_ref(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<String>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_element_id_ref(inner, path, plan, context)
        }
        Expression::FunctionCall(function) if is_element_id_function(function) => Ok(Some(
            compile_element_id_variable(function, path, plan, context)?,
        )),
        _ => Ok(None),
    }
}

fn compile_id_variable(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    let variable = compile_single_variable_function_argument(
        function,
        format!("{path}.arguments"),
        "id() supports exactly one graph variable argument",
        context,
    )?;
    if !plan_uses_variable(plan, &variable) {
        return Err(unsupported(
            format!("{path}.arguments[0]"),
            format!("id() argument '{variable}' is not a bound graph variable"),
        ));
    }
    Ok(variable)
}

fn compile_element_id_variable(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    let variable = compile_single_variable_function_argument(
        function,
        format!("{path}.arguments"),
        "elementId() supports exactly one graph variable argument",
        context,
    )?;
    if !plan_uses_variable(plan, &variable) {
        return Err(unsupported(
            format!("{path}.arguments[0]"),
            format!("elementId() argument '{variable}' is not a bound graph variable"),
        ));
    }
    Ok(variable)
}

fn compile_predicate_literal(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Literal, CoreError> {
    compile_predicate_literal_in_mode(
        expression,
        path,
        PredicateCompileMode::Graph { plan },
        context,
    )
}

fn compile_predicate_literal_in_mode(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Literal, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_predicate_literal_in_mode(inner, path, mode, context)
        }
        Expression::FunctionCall(function) if is_type_function(function) => {
            match mode.static_metadata_plan() {
                Some(plan) => compile_type_literal(function, path, plan, context),
                None => Err(unsupported(path, "type() operands require graph context")),
            }
        }
        Expression::FunctionCall(function)
            if matches!(mode, PredicateCompileMode::CaseWhen { .. })
                && (is_id_function(function)
                    || is_element_id_function(function)
                    || is_labels_function(function)
                    || is_keys_function(function)) =>
        {
            Err(unsupported(
                path,
                "CASE WHEN predicates do not support graph identity or metadata functions yet",
            ))
        }
        _ => compile_literal(expression, path, context),
    }
}

fn compile_type_literal(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let variable = compile_single_variable_function_argument(
        function,
        format!("{path}.arguments"),
        "type() supports exactly one relationship variable argument",
        context,
    )?;
    let relationship = plan
        .relationships
        .iter()
        .find(|relationship| relationship.variable.as_deref() == Some(variable.as_str()))
        .ok_or_else(|| {
            unsupported(
                format!("{path}.arguments[0]"),
                format!("type() argument '{variable}' is not a named relationship variable"),
            )
        })?;
    Ok(Literal::String(relationship.relationship_type.clone()))
}

fn contains_type_function(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => contains_type_function(inner),
        Expression::FunctionCall(function) => is_type_function(function),
        _ => false,
    }
}

fn evaluate_literal_comparison(
    lhs: &Literal,
    operator: ComparisonOperator,
    rhs: &Literal,
    path: impl Into<String>,
) -> Result<bool, CoreError> {
    let path = path.into();
    match operator {
        ComparisonOperator::Equal => match compare_numeric_literals(lhs, rhs, path.clone())? {
            Some(ordering) => Ok(ordering == Ordering::Equal),
            None => Ok(lhs == rhs),
        },
        ComparisonOperator::NotEqual => match compare_numeric_literals(lhs, rhs, path.clone())? {
            Some(ordering) => Ok(ordering != Ordering::Equal),
            None => Ok(lhs != rhs),
        },
        ComparisonOperator::StartsWith => match (lhs, rhs) {
            (Literal::String(lhs), Literal::String(rhs)) => Ok(lhs.starts_with(rhs)),
            _ => Err(unsupported(
                path,
                "STARTS WITH literal comparisons require string operands",
            )),
        },
        ComparisonOperator::EndsWith => match (lhs, rhs) {
            (Literal::String(lhs), Literal::String(rhs)) => Ok(lhs.ends_with(rhs)),
            _ => Err(unsupported(
                path,
                "ENDS WITH literal comparisons require string operands",
            )),
        },
        ComparisonOperator::Contains => match (lhs, rhs) {
            (Literal::String(lhs), Literal::String(rhs)) => Ok(lhs.contains(rhs)),
            _ => Err(unsupported(
                path,
                "CONTAINS literal comparisons require string operands",
            )),
        },
        ComparisonOperator::RegexMatch => match (lhs, rhs) {
            (Literal::String(lhs), Literal::String(rhs)) => Regex::new(rhs)
                .map(|pattern| pattern.is_match(lhs))
                .map_err(|error| {
                    unsupported(
                        path,
                        format!("invalid regex literal for =~ comparison: {error}"),
                    )
                }),
            _ => Err(unsupported(
                path,
                "=~ literal comparisons require string operands",
            )),
        },
        ComparisonOperator::GreaterThan
        | ComparisonOperator::GreaterThanOrEqual
        | ComparisonOperator::LessThan
        | ComparisonOperator::LessThanOrEqual => {
            let Some(ordering) = compare_numeric_literals(lhs, rhs, path.clone())? else {
                return Err(unsupported(
                    path,
                    "ordered literal comparisons require numeric operands",
                ));
            };
            match operator {
                ComparisonOperator::GreaterThan => Ok(ordering == Ordering::Greater),
                ComparisonOperator::GreaterThanOrEqual => {
                    Ok(matches!(ordering, Ordering::Greater | Ordering::Equal))
                }
                ComparisonOperator::LessThan => Ok(ordering == Ordering::Less),
                ComparisonOperator::LessThanOrEqual => {
                    Ok(matches!(ordering, Ordering::Less | Ordering::Equal))
                }
                _ => unreachable!("non-ordered operator reached ordered comparison branch"),
            }
        }
        ComparisonOperator::In => Err(unsupported(
            path,
            "literal comparisons do not use the IN comparison operator",
        )),
    }
}

fn evaluate_literal_only_comparison(
    lhs: &Literal,
    operator: ComparisonOperator,
    rhs: &Literal,
    path: impl Into<String>,
) -> Result<bool, CoreError> {
    let path = path.into();
    if matches!(lhs, Literal::Null) || matches!(rhs, Literal::Null) {
        return Err(unsupported(
            path,
            "literal-only null comparisons are not supported because Cypher null comparisons produce unknown",
        ));
    }
    evaluate_literal_comparison(lhs, operator, rhs, path)
}

fn evaluate_literal_in_list(
    literal: &Literal,
    literals: &[Literal],
    path: impl Into<String>,
) -> Result<bool, CoreError> {
    let path = path.into();
    if matches!(literal, Literal::Null) {
        return Err(unsupported(
            path,
            "literal IN predicates with a null left-hand side are not supported because Cypher membership produces unknown",
        ));
    }

    let mut saw_null = false;
    for candidate in literals {
        if matches!(candidate, Literal::Null) {
            saw_null = true;
            continue;
        }
        if evaluate_literal_comparison(literal, ComparisonOperator::Equal, candidate, path.clone())?
        {
            return Ok(true);
        }
    }

    if saw_null {
        return Err(unsupported(
            path,
            "literal IN predicates with null members cannot be folded unless a non-null match is found",
        ));
    }

    Ok(false)
}

fn compare_numeric_literals(
    lhs: &Literal,
    rhs: &Literal,
    path: impl Into<String>,
) -> Result<Option<Ordering>, CoreError> {
    let path = path.into();
    match (lhs, rhs) {
        (Literal::Integer(lhs), Literal::Integer(rhs)) => Ok(Some(lhs.cmp(rhs))),
        (Literal::Float(lhs), Literal::Float(rhs)) => lhs
            .into_inner()
            .partial_cmp(&rhs.into_inner())
            .map(Some)
            .ok_or_else(|| unsupported(path, "non-finite numeric literals are not supported")),
        (Literal::Integer(lhs), Literal::Float(rhs)) => {
            compare_integer_float_literals(*lhs, rhs.into_inner(), path).map(Some)
        }
        (Literal::Float(lhs), Literal::Integer(rhs)) => {
            compare_integer_float_literals(*rhs, lhs.into_inner(), path)
                .map(Ordering::reverse)
                .map(Some)
        }
        _ => Ok(None),
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "integer is range-checked to f64's exact integer range before casting"
)]
fn compare_integer_float_literals(
    integer: i64,
    float: f64,
    path: impl Into<String>,
) -> Result<Ordering, CoreError> {
    const MAX_EXACT_F64_INTEGER: i64 = 9_007_199_254_740_992;
    let path = path.into();
    if !(-MAX_EXACT_F64_INTEGER..=MAX_EXACT_F64_INTEGER).contains(&integer) {
        return Err(unsupported(
            path,
            "mixed integer/float literal comparisons require an integer that can be represented exactly as f64",
        ));
    }
    // The range guard above restricts the integer to f64's exact integer range.
    (integer as f64)
        .partial_cmp(&float)
        .ok_or_else(|| unsupported(path, "non-finite numeric literals are not supported"))
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
    context: &CypherCompileContext,
) -> Result<Vec<Literal>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_literal_list(inner, path, context),
        Expression::Literal(CypherLiteral::List(list)) => list
            .elements
            .iter()
            .enumerate()
            .map(|(index, expression)| {
                compile_literal(expression, format!("{path}[{index}]"), context)
            })
            .collect(),
        Expression::Parameter(parameter) => {
            match context.parameter_value(parameter, path.clone())? {
                CypherParameterValue::List(values) => Ok(values.clone()),
                CypherParameterValue::Literal(_) => Err(unsupported(
                    path,
                    "IN parameter right-hand sides require a list value",
                )),
            }
        }
        _ => Err(unsupported(
            path,
            "IN predicates require a literal list right-hand side",
        )),
    }
}

fn compile_literal(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Literal, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_literal(inner, path, context),
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
        } => match compile_literal(operand, path, context)? {
            Literal::Integer(value) => Ok(Literal::Integer(-value)),
            Literal::Float(value) => Ok(Literal::Float(OrderedFloat(-value.into_inner()))),
            _ => Err(unsupported(
                "literal",
                "only numeric literals can be negated",
            )),
        },
        Expression::Parameter(parameter) => {
            match context.parameter_value(parameter, path.clone())? {
                CypherParameterValue::Literal(value) => Ok(value.clone()),
                CypherParameterValue::List(_) => Err(unsupported(
                    path,
                    "list parameters can only be used as IN right-hand sides",
                )),
            }
        }
        _ => Err(unsupported(
            path,
            "only string, numeric, boolean, and null literals are supported",
        )),
    }
}

fn validate_literal_list_projection(
    literals: &[Literal],
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    if literals.is_empty() {
        return Err(unsupported(
            path,
            "literal list projections require at least one element",
        ));
    }

    let mut expected = None;
    for literal in literals {
        let Some(kind) = literal_list_element_kind(literal) else {
            continue;
        };
        match expected {
            Some(expected) if expected != kind => {
                return Err(unsupported(
                    path,
                    "literal list projections require all non-null elements to have the same type",
                ));
            }
            Some(_) => {}
            None => expected = Some(kind),
        }
    }

    if expected.is_none() {
        return Err(unsupported(
            path,
            "literal list projections require at least one non-null element",
        ));
    }

    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LiteralListElementKind {
    String,
    Integer,
    Float,
    Boolean,
}

fn literal_list_element_kind(literal: &Literal) -> Option<LiteralListElementKind> {
    match literal {
        Literal::String(_) => Some(LiteralListElementKind::String),
        Literal::Integer(_) => Some(LiteralListElementKind::Integer),
        Literal::Float(_) => Some(LiteralListElementKind::Float),
        Literal::Boolean(_) => Some(LiteralListElementKind::Boolean),
        Literal::Null => None,
    }
}

fn is_literal_projection_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_literal_projection_expression(inner),
        Expression::Literal(_) | Expression::Parameter(_) => true,
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            operand,
            ..
        } => is_literal_expression(operand),
        _ => false,
    }
}

fn is_literal_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_literal_expression(inner),
        Expression::Literal(CypherLiteral::List(_)) => false,
        Expression::Literal(_) | Expression::Parameter(_) => true,
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            operand,
            ..
        } => is_literal_expression(operand),
        _ => false,
    }
}

fn is_arithmetic_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_arithmetic_expression(inner),
        Expression::BinaryOp { op, .. } => matches!(
            op,
            CypherBinaryOperator::Add
                | CypherBinaryOperator::Subtract
                | CypherBinaryOperator::Multiply
                | CypherBinaryOperator::Divide
                | CypherBinaryOperator::Modulo
                | CypherBinaryOperator::Power
        ),
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            ..
        } => true,
        _ => false,
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

fn compile_limit(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<u64, CoreError> {
    compile_non_negative_integer(expression, path, "LIMIT", context)
}

fn compile_skip(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<u64, CoreError> {
    compile_non_negative_integer(expression, path, "SKIP", context)
}

fn compile_non_negative_integer(
    expression: &Expression,
    path: impl Into<String>,
    keyword: &str,
    context: &CypherCompileContext,
) -> Result<u64, CoreError> {
    let path = path.into();
    match compile_literal(expression, path.clone(), context)? {
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

fn compile_comparison_operator(operator: CypherComparisonOperator) -> ComparisonOperator {
    match operator {
        CypherComparisonOperator::Eq => ComparisonOperator::Equal,
        CypherComparisonOperator::Ne => ComparisonOperator::NotEqual,
        CypherComparisonOperator::Gt => ComparisonOperator::GreaterThan,
        CypherComparisonOperator::Ge => ComparisonOperator::GreaterThanOrEqual,
        CypherComparisonOperator::Lt => ComparisonOperator::LessThan,
        CypherComparisonOperator::Le => ComparisonOperator::LessThanOrEqual,
        CypherComparisonOperator::StartsWith => ComparisonOperator::StartsWith,
        CypherComparisonOperator::EndsWith => ComparisonOperator::EndsWith,
        CypherComparisonOperator::Contains => ComparisonOperator::Contains,
        CypherComparisonOperator::RegexMatch => ComparisonOperator::RegexMatch,
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
        | ComparisonOperator::Contains
        | ComparisonOperator::RegexMatch => Err(unsupported(
            path,
            "this comparison operator requires a variable.property left-hand side",
        )),
    }
}

fn is_string_comparison_operator(operator: ComparisonOperator) -> bool {
    matches!(
        operator,
        ComparisonOperator::StartsWith
            | ComparisonOperator::EndsWith
            | ComparisonOperator::Contains
            | ComparisonOperator::RegexMatch
    )
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

fn fresh_internal_node_variable(plan: &GraphPlan, part_index: usize, node_index: usize) -> String {
    let mut suffix = 0;
    loop {
        let candidate = if suffix == 0 {
            format!("__coral_node_{part_index}_{node_index}")
        } else {
            format!("__coral_node_{part_index}_{node_index}_{suffix}")
        };
        if !plan_uses_variable(plan, &candidate) {
            return candidate;
        }
        suffix += 1;
    }
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
    fn compiles_transparent_with_where_pass_through() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WITH service \
             WHERE service.tier = 'prod' \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS source, target.name AS target",
        )
        .expect("transparent WITH WHERE query should compile");

        assert_eq!(plan.nodes.len(), 2);
        assert_eq!(plan.relationships.len(), 1);
        assert_eq!(
            plan.predicates,
            vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            }]
        );
    }

    #[test]
    fn compiles_transparent_with_variable_aliases() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WITH service AS s \
             WHERE s.tier = 'prod' \
             MATCH (s)-[:DEPENDS_ON]->(target:Service) \
             RETURN s.name AS source, target.name AS target",
        )
        .expect("transparent WITH aliases should compile");

        assert_eq!(
            plan.nodes,
            vec![
                NodePattern {
                    variable: "s".to_string(),
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
            vec![RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "s".to_string(),
                direction: Direction::Outgoing,
                right: "target".to_string(),
            }]
        );
        assert_eq!(
            plan.predicates,
            vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "s".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            }]
        );
        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "s".to_string(),
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
    fn compiles_transparent_with_relationship_variable_aliases() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WITH person AS p, owns AS rel, service AS s \
             RETURN p.name AS owner, type(rel) AS relationship_type, s.name AS service",
        )
        .expect("transparent WITH relationship aliases should compile");

        assert_eq!(
            plan.nodes,
            vec![
                NodePattern {
                    variable: "p".to_string(),
                    label: "Person".to_string(),
                },
                NodePattern {
                    variable: "s".to_string(),
                    label: "Service".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.relationships,
            vec![RelationshipPattern {
                variable: Some("rel".to_string()),
                relationship_type: "OWNS".to_string(),
                left: "p".to_string(),
                direction: Direction::Outgoing,
                right: "s".to_string(),
            }]
        );
        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "p".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("owner".to_string()),
                },
                Projection::RelationshipType {
                    variable: "rel".to_string(),
                    relationship_type: "OWNS".to_string(),
                    alias: "relationship_type".to_string(),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "s".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("service".to_string()),
                },
            ]
        );
    }

    #[test]
    fn compiles_transparent_with_star_pass_through() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WITH * \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS source, target.name AS target",
        )
        .expect("transparent WITH * query should compile");

        assert_eq!(plan.nodes.len(), 2);
        assert_eq!(plan.relationships.len(), 1);
        assert_eq!(plan.projections.len(), 2);
    }

    #[test]
    fn compiles_transparent_with_star_where_pass_through() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WITH * \
             WHERE service.active = true \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS source, target.name AS target",
        )
        .expect("transparent WITH * WHERE query should compile");

        assert_eq!(plan.nodes.len(), 2);
        assert_eq!(plan.relationships.len(), 1);
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
    fn compiles_terminal_with_final_return_aliases() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WITH service.tier AS tier, count(service) AS services \
             RETURN tier AS service_tier, services AS total_services \
             ORDER BY total_services DESC, service_tier",
        )
        .expect("terminal WITH final RETURN aliases should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    },
                    alias: Some("service_tier".to_string()),
                },
                Projection::Aggregate {
                    function: AggregateFunction::Count,
                    target: AggregateTarget::VariableKey {
                        variable: "service".to_string(),
                    },
                    distinct: false,
                    alias: "total_services".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.order_by,
            vec![
                OrderKey {
                    expression: OrderExpression::ProjectionAlias("total_services".to_string()),
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
    }

    #[test]
    fn compiles_terminal_with_scalar_where_alias_predicates() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WITH person.name AS owner, service.tier AS tier \
             WHERE owner STARTS WITH 'Ada' AND tier IN ['prod', 'critical'] \
             RETURN owner, tier",
        )
        .expect("terminal WITH scalar WHERE should compile");

        assert_eq!(
            plan.post_projection_predicate,
            Some(ProjectionPredicateExpression::And {
                left: Box::new(ProjectionPredicateExpression::Comparison(
                    ProjectionPredicate {
                        alias: "owner".to_string(),
                        operator: ComparisonOperator::StartsWith,
                        rhs: ProjectionPredicateRhs::Literal(Literal::String("Ada".to_string())),
                    },
                )),
                right: Box::new(ProjectionPredicateExpression::Comparison(
                    ProjectionPredicate {
                        alias: "tier".to_string(),
                        operator: ComparisonOperator::In,
                        rhs: ProjectionPredicateRhs::List(vec![
                            Literal::String("prod".to_string()),
                            Literal::String("critical".to_string()),
                        ]),
                    },
                )),
            })
        );
    }

    #[test]
    fn compiles_terminal_with_aggregate_where_alias_predicates() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WITH person.team AS team, count(service) AS services \
             WHERE services > 1 AND team IS NOT NULL \
             RETURN team, services",
        )
        .expect("terminal WITH aggregate WHERE should compile");

        assert_eq!(
            plan.post_projection_predicate,
            Some(ProjectionPredicateExpression::And {
                left: Box::new(ProjectionPredicateExpression::Comparison(
                    ProjectionPredicate {
                        alias: "services".to_string(),
                        operator: ComparisonOperator::GreaterThan,
                        rhs: ProjectionPredicateRhs::Literal(Literal::Integer(1)),
                    },
                )),
                right: Box::new(ProjectionPredicateExpression::Comparison(
                    ProjectionPredicate {
                        alias: "team".to_string(),
                        operator: ComparisonOperator::NotEqual,
                        rhs: ProjectionPredicateRhs::Literal(Literal::Null),
                    },
                )),
            })
        );
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
    fn compiles_terminal_with_graph_variable_modifiers() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WITH service AS s \
             ORDER BY s.risk DESC \
             SKIP 1 \
             LIMIT 2 \
             RETURN s.name AS service, s.risk AS risk",
        )
        .expect("terminal WITH graph variable modifiers should compile");

        assert_eq!(
            plan.nodes,
            vec![NodePattern {
                variable: "s".to_string(),
                label: "Service".to_string(),
            }]
        );
        assert_eq!(plan.predicates, Vec::new());
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "s".to_string(),
                    property: "risk".to_string(),
                }),
                direction: OrderDirection::Descending,
            }]
        );
        assert_eq!(plan.skip, Some(1));
        assert_eq!(plan.limit, Some(2));
        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "s".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("service".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "s".to_string(),
                        property: "risk".to_string(),
                    },
                    alias: Some("risk".to_string()),
                },
            ]
        );
    }

    #[test]
    fn compiles_terminal_with_star_modifiers() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WITH * \
             ORDER BY service.risk DESC \
             LIMIT 1 \
             RETURN service.name AS service",
        )
        .expect("terminal WITH * modifiers should compile");

        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                direction: OrderDirection::Descending,
            }]
        );
        assert_eq!(plan.limit, Some(1));
        assert_eq!(
            plan.projections,
            vec![Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            }]
        );
    }

    #[test]
    fn compiles_id_and_type_projections() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN id(person) AS person_id, id(owns) AS ownership_id, type(owns) AS relationship_type \
             ORDER BY ownership_id",
        )
        .expect("id() and type() projections should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Key {
                    variable: "person".to_string(),
                    alias: "person_id".to_string(),
                },
                Projection::Key {
                    variable: "owns".to_string(),
                    alias: "ownership_id".to_string(),
                },
                Projection::RelationshipType {
                    variable: "owns".to_string(),
                    relationship_type: "OWNS".to_string(),
                    alias: "relationship_type".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::ProjectionAlias("ownership_id".to_string()),
                direction: OrderDirection::Ascending,
            }]
        );
    }

    #[test]
    fn compiles_element_id_projections() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN elementId(person) AS person_element_id, elementId(owns) AS ownership_element_id \
             ORDER BY ownership_element_id",
        )
        .expect("elementId() projections should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::ElementId {
                    variable: "person".to_string(),
                    alias: "person_element_id".to_string(),
                },
                Projection::ElementId {
                    variable: "owns".to_string(),
                    alias: "ownership_element_id".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::ProjectionAlias("ownership_element_id".to_string()),
                direction: OrderDirection::Ascending,
            }]
        );
    }

    #[test]
    fn compiles_labels_projection() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN labels(service) AS service_labels \
             ORDER BY service_labels",
        )
        .expect("labels() projection should compile");

        assert_eq!(
            plan.projections,
            vec![Projection::NodeLabels {
                variable: "service".to_string(),
                label: "Service".to_string(),
                alias: "service_labels".to_string(),
            }]
        );
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::ProjectionAlias("service_labels".to_string()),
                direction: OrderDirection::Ascending,
            }]
        );
    }

    #[test]
    fn compiles_order_by_id_and_type_functions() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN person.name AS owner \
             ORDER BY id(person), id(owns) DESC, type(owns)",
        )
        .expect("id() and type() order expressions should compile");

        assert_eq!(
            plan.order_by,
            vec![
                OrderKey {
                    expression: OrderExpression::Key {
                        variable: "person".to_string(),
                    },
                    direction: OrderDirection::Ascending,
                },
                OrderKey {
                    expression: OrderExpression::Key {
                        variable: "owns".to_string(),
                    },
                    direction: OrderDirection::Descending,
                },
                OrderKey {
                    expression: OrderExpression::RelationshipType {
                        variable: "owns".to_string(),
                        relationship_type: "OWNS".to_string(),
                    },
                    direction: OrderDirection::Ascending,
                },
            ]
        );
    }

    #[test]
    fn compiles_order_by_element_id_function() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN person.name AS owner \
             ORDER BY elementId(person), elementId(owns) DESC",
        )
        .expect("elementId() order expressions should compile");

        assert_eq!(
            plan.order_by,
            vec![
                OrderKey {
                    expression: OrderExpression::ElementId {
                        variable: "person".to_string(),
                    },
                    direction: OrderDirection::Ascending,
                },
                OrderKey {
                    expression: OrderExpression::ElementId {
                        variable: "owns".to_string(),
                    },
                    direction: OrderDirection::Descending,
                },
            ]
        );
    }

    #[test]
    fn compiles_order_by_labels_function() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY labels(service) DESC",
        )
        .expect("labels() order expression should compile");

        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::NodeLabels {
                    variable: "service".to_string(),
                    label: "Service".to_string(),
                },
                direction: OrderDirection::Descending,
            }]
        );
    }

    #[test]
    fn compiles_order_by_keys_function() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN service.name AS service \
             ORDER BY keys(service) DESC, keys(owns)",
        )
        .expect("keys() order expressions should compile");

        assert_eq!(
            plan.order_by,
            vec![
                OrderKey {
                    expression: OrderExpression::PropertyKeys {
                        variable: "service".to_string(),
                    },
                    direction: OrderDirection::Descending,
                },
                OrderKey {
                    expression: OrderExpression::PropertyKeys {
                        variable: "owns".to_string(),
                    },
                    direction: OrderDirection::Ascending,
                },
            ]
        );
    }

    #[test]
    fn rejects_labels_on_relationship_variables() {
        let error = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN labels(owns) AS labels",
        )
        .expect_err("labels() should require a node variable");

        assert!(
            error.to_string().contains("labels() argument 'owns'"),
            "{error:?}"
        );
    }

    #[test]
    fn compiles_keys_projection() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN keys(person) AS person_keys, keys(owns) AS ownership_keys",
        )
        .expect("keys() projections should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::PropertyKeys {
                    variable: "person".to_string(),
                    alias: "person_keys".to_string(),
                },
                Projection::PropertyKeys {
                    variable: "owns".to_string(),
                    alias: "ownership_keys".to_string(),
                },
            ]
        );
    }

    #[test]
    fn compiles_property_key_membership_predicates() {
        let parameters = BTreeMap::from([(
            "relationship_key".to_string(),
            CypherParameterValue::Literal(Literal::String("since".to_string())),
        )]);
        let plan = compile_cypher_with_parameters(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE 'name' IN keys(person) AND $relationship_key IN keys(owns) \
             RETURN person.name AS owner",
            &parameters,
        )
        .expect("keys() membership predicates should compile");

        assert_eq!(plan.predicates, Vec::new());
        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::And {
                left: Box::new(PredicateExpression::PropertyKeyMembership(
                    PropertyKeyMembershipPredicate {
                        variable: "person".to_string(),
                        key: "name".to_string(),
                    },
                )),
                right: Box::new(PredicateExpression::PropertyKeyMembership(
                    PropertyKeyMembershipPredicate {
                        variable: "owns".to_string(),
                        key: "since".to_string(),
                    },
                )),
            })
        );
    }

    #[test]
    fn rejects_non_string_property_key_membership_predicates() {
        let error = compile_cypher(
            "MATCH (service:Service) \
             WHERE 1 IN keys(service) \
             RETURN service.name",
        )
        .expect_err("keys() membership should require a string literal");

        assert!(
            error.to_string().contains("keys() membership predicates"),
            "{error:?}"
        );
    }

    #[test]
    fn compiles_id_predicates() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE id(person) = 1 AND id(owns) IN [100, 200] \
             RETURN person.name AS owner",
        )
        .expect("id() predicates should compile");

        assert_eq!(plan.predicates, Vec::new());
        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::And {
                left: Box::new(PredicateExpression::KeyComparison(KeyPredicate {
                    variable: "person".to_string(),
                    operator: ComparisonOperator::Equal,
                    rhs: PredicateRhs::Literal(Literal::Integer(1)),
                })),
                right: Box::new(PredicateExpression::KeyComparison(KeyPredicate {
                    variable: "owns".to_string(),
                    operator: ComparisonOperator::In,
                    rhs: PredicateRhs::List(vec![Literal::Integer(100), Literal::Integer(200)]),
                })),
            })
        );
    }

    #[test]
    fn compiles_element_id_predicates() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE elementId(person) = '1' AND elementId(owns) IN ['100', '200'] \
             RETURN person.name AS owner",
        )
        .expect("elementId() predicates should compile");

        assert_eq!(plan.predicates, Vec::new());
        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::And {
                left: Box::new(PredicateExpression::ElementIdComparison(
                    ElementIdPredicate {
                        variable: "person".to_string(),
                        operator: ComparisonOperator::Equal,
                        rhs: PredicateRhs::Literal(Literal::String("1".to_string())),
                    },
                )),
                right: Box::new(PredicateExpression::ElementIdComparison(
                    ElementIdPredicate {
                        variable: "owns".to_string(),
                        operator: ComparisonOperator::In,
                        rhs: PredicateRhs::List(vec![
                            Literal::String("100".to_string()),
                            Literal::String("200".to_string()),
                        ]),
                    },
                )),
            })
        );
    }

    #[test]
    fn compiles_type_predicates_as_boolean_constants() {
        let matching = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE type(owns) = 'OWNS' \
             RETURN person.name AS owner",
        )
        .expect("matching type() predicate should compile");
        let non_matching = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE type(owns) = 'DEPENDS_ON' \
             RETURN person.name AS owner",
        )
        .expect("non-matching type() predicate should compile");
        let string_matching = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE type(owns) STARTS WITH 'OW' \
                AND type(owns) ENDS WITH 'NS' \
                AND type(owns) CONTAINS 'WN' \
                AND type(owns) =~ '^OW.*' \
             RETURN person.name AS owner",
        )
        .expect("matching type() string predicates should compile");
        let string_non_matching = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE type(owns) STARTS WITH 'DEP' \
             RETURN person.name AS owner",
        )
        .expect("non-matching type() string predicate should compile");

        assert_eq!(matching.predicate, Some(PredicateExpression::Boolean(true)));
        assert_eq!(
            non_matching.predicate,
            Some(PredicateExpression::Boolean(false))
        );
        assert!(matches!(
            string_matching.predicate,
            Some(PredicateExpression::And { .. })
        ));
        assert_eq!(
            string_non_matching.predicate,
            Some(PredicateExpression::Boolean(false))
        );
    }

    #[test]
    fn compiles_label_membership_predicates_as_boolean_constants() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE 'Service' IN labels(service) AND NOT ('Team' IN labels(service)) \
             RETURN service.name AS service",
        )
        .expect("labels() membership predicates should compile");

        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::And {
                left: Box::new(PredicateExpression::Boolean(true)),
                right: Box::new(PredicateExpression::Not {
                    expression: Box::new(PredicateExpression::Boolean(false)),
                }),
            })
        );
    }

    #[test]
    fn compiles_parameterized_label_membership_predicates() {
        let parameters = BTreeMap::from([(
            "label".to_string(),
            CypherParameterValue::Literal(Literal::String("Service".to_string())),
        )]);
        let plan = compile_cypher_with_parameters(
            "MATCH (service:Service) \
             WHERE $label IN labels(service) \
             RETURN service.name AS service",
            &parameters,
        )
        .expect("parameterized labels() membership should compile");

        assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
    }

    #[test]
    fn rejects_non_string_label_membership_predicates() {
        let error = compile_cypher(
            "MATCH (service:Service) \
             WHERE 1 IN labels(service) \
             RETURN service.name AS service",
        )
        .expect_err("label membership should require a string literal");

        assert!(
            error.to_string().contains("label membership predicates"),
            "{error:?}"
        );
    }

    #[test]
    fn compiles_node_label_predicates_as_boolean_constants() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE service:Service AND NOT (service:Team) \
             RETURN service.name AS service",
        )
        .expect("node label predicates should compile");

        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::And {
                left: Box::new(PredicateExpression::Boolean(true)),
                right: Box::new(PredicateExpression::Not {
                    expression: Box::new(PredicateExpression::Boolean(false)),
                }),
            })
        );
    }

    #[test]
    fn compiles_compound_node_label_predicates_as_boolean_constants() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE service:Service|Team \
             RETURN service.name AS service",
        )
        .expect("compound node label predicates should compile");

        assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
    }

    #[test]
    fn compiles_relationship_type_predicates_as_boolean_constants() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE owns:OWNS AND NOT (owns:DEPENDS_ON) \
             RETURN service.name AS service",
        )
        .expect("relationship type predicates should compile");

        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::And {
                left: Box::new(PredicateExpression::Boolean(true)),
                right: Box::new(PredicateExpression::Not {
                    expression: Box::new(PredicateExpression::Boolean(false)),
                }),
            })
        );
    }

    #[test]
    fn compiles_boolean_scalar_projections() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN service.risk > 0.8 AS high_risk, \
                    service.tier IS NULL AS missing_tier, \
                    service.name =~ '^billing.*' AS billing_service, \
                    service:Service AS is_service, \
                    owns:OWNS AS is_ownership, \
                    'tier' IN keys(service) AS has_tier \
             ORDER BY service.risk > 0.8 DESC",
        )
        .expect("boolean scalar projections should compile");

        assert_eq!(plan.projections.len(), 6);
        assert!(plan.projections.iter().all(|projection| {
            matches!(
                projection,
                Projection::Expression {
                    expression: ScalarExpression::Predicate(_),
                    ..
                }
            )
        }));
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Predicate(_)),
                direction: OrderDirection::Descending,
            }]
        ));
    }

    #[test]
    fn rejects_dynamic_node_label_predicates() {
        let error = compile_cypher(
            "MATCH (service:Service) \
             WHERE service:$(label) \
             RETURN service.name AS service",
        )
        .expect_err("dynamic node label predicates should be rejected");

        assert!(
            error.to_string().contains("dynamic label predicates"),
            "{error:?}"
        );
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
    fn compiles_literal_projections() {
        let parameters = BTreeMap::from([(
            "kind".to_string(),
            CypherParameterValue::Literal(Literal::String("service".to_string())),
        )]);
        let plan = compile_cypher_with_parameters(
            "MATCH (service:Service) \
             RETURN $kind AS kind, 1 AS version, true AS enabled, null AS missing, -1.5 AS score \
             ORDER BY 'constant'",
            &parameters,
        )
        .expect("literal projections should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Literal {
                    literal: Literal::String("service".to_string()),
                    alias: "kind".to_string(),
                },
                Projection::Literal {
                    literal: Literal::Integer(1),
                    alias: "version".to_string(),
                },
                Projection::Literal {
                    literal: Literal::Boolean(true),
                    alias: "enabled".to_string(),
                },
                Projection::Literal {
                    literal: Literal::Null,
                    alias: "missing".to_string(),
                },
                Projection::Literal {
                    literal: Literal::Float(OrderedFloat(-1.5)),
                    alias: "score".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Literal(Literal::String("constant".to_string())),
                direction: OrderDirection::Ascending,
            }]
        );
    }

    #[test]
    fn compiles_literal_list_projections() {
        let parameters = BTreeMap::from([(
            "selected_tiers".to_string(),
            CypherParameterValue::List(vec![Literal::String("prod".to_string()), Literal::Null]),
        )]);
        let plan = compile_cypher_with_parameters(
            "MATCH (service:Service) \
             RETURN ['prod', 'dev'] AS tiers, $selected_tiers AS selected_tiers",
            &parameters,
        )
        .expect("literal list projections should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::LiteralList {
                    literals: vec![
                        Literal::String("prod".to_string()),
                        Literal::String("dev".to_string()),
                    ],
                    alias: "tiers".to_string(),
                },
                Projection::LiteralList {
                    literals: vec![Literal::String("prod".to_string()), Literal::Null,],
                    alias: "selected_tiers".to_string(),
                },
            ]
        );
    }

    #[test]
    fn compiles_coalesce_projection() {
        let parameters = BTreeMap::from([(
            "fallback".to_string(),
            CypherParameterValue::Literal(Literal::String("unassigned".to_string())),
        )]);
        let plan = compile_cypher_with_parameters(
            "MATCH (service:Service) \
             RETURN coalesce(service.team, service.tier, $fallback) AS owner_team",
            &parameters,
        )
        .expect("coalesce projection should compile");

        assert_eq!(
            plan.projections,
            vec![Projection::Expression {
                expression: ScalarExpression::Coalesce {
                    expressions: vec![
                        ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "team".to_string(),
                        }),
                        ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "tier".to_string(),
                        }),
                        ScalarExpression::Literal(Literal::String("unassigned".to_string())),
                    ],
                },
                alias: "owner_team".to_string(),
            }]
        );
    }

    #[test]
    fn compiles_null_if_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE nullIf(service.tier, 'dev') IS NULL \
             RETURN nullIf(service.tier, 'prod') AS normalized_tier \
             ORDER BY nullIf(service.team, service.tier)",
        )
        .expect("nullIf scalar expressions should compile");

        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::NullIf {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    })),
                    value: Box::new(ScalarExpression::Literal(Literal::String(
                        "dev".to_string()
                    ))),
                },
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null)),
            }))
        );
        assert_eq!(
            plan.projections,
            vec![Projection::Expression {
                expression: ScalarExpression::NullIf {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    })),
                    value: Box::new(ScalarExpression::Literal(Literal::String(
                        "prod".to_string()
                    ))),
                },
                alias: "normalized_tier".to_string(),
            }]
        );
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::NullIf { .. }),
                direction: OrderDirection::Ascending,
            }]
        ));
    }

    #[test]
    fn rejects_null_if_with_unsupported_arity() {
        let error = compile_cypher(
            "MATCH (service:Service) \
             RETURN nullIf(service.tier) AS normalized_tier",
        )
        .expect_err("nullIf() requires exactly two arguments");

        assert!(
            error
                .to_string()
                .contains("nullIf() requires exactly two arguments"),
            "{error}"
        );
    }

    #[test]
    fn compiles_order_by_coalesce_expression() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY coalesce(service.tier, 'unassigned') DESC",
        )
        .expect("coalesce order expression should compile");

        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Coalesce {
                    expressions: vec![
                        ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "tier".to_string(),
                        }),
                        ScalarExpression::Literal(Literal::String("unassigned".to_string())),
                    ],
                }),
                direction: OrderDirection::Descending,
            }]
        );
    }

    #[test]
    fn compiles_to_string_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE toString(service.risk) STARTS WITH '0.9' \
             RETURN toString(service.risk) AS risk_text \
             ORDER BY toString(service.risk)",
        )
        .expect("toString scalar expressions should compile");

        let expected_expression = ScalarExpression::ToString {
            expression: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            })),
        };
        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: expected_expression.clone(),
                operator: ComparisonOperator::StartsWith,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                    "0.9".to_string()
                ))),
            }))
        );
        assert_eq!(
            plan.projections,
            vec![Projection::Expression {
                expression: expected_expression.clone(),
                alias: "risk_text".to_string(),
            }]
        );
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Scalar(expected_expression),
                direction: OrderDirection::Ascending,
            }]
        );
    }

    #[test]
    fn compiles_string_case_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE toLower(service.name) CONTAINS 'api' \
             RETURN toUpper(service.tier) AS tier_upper \
             ORDER BY toLower(service.name)",
        )
        .expect("string case scalar expressions should compile");

        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::ToLower {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                },
                operator: ComparisonOperator::Contains,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                    "api".to_string()
                ))),
            }))
        );
        assert_eq!(
            plan.projections,
            vec![Projection::Expression {
                expression: ScalarExpression::ToUpper {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    })),
                },
                alias: "tier_upper".to_string(),
            }]
        );
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::ToLower { .. }),
                direction: OrderDirection::Ascending,
            }]
        ));
    }

    #[test]
    fn compiles_trim_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE trim(service.tier) = 'prod' \
             RETURN lTrim(service.name) AS left_trimmed \
             ORDER BY rTrim(service.name)",
        )
        .expect("trim scalar expressions should compile");

        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Trim {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    })),
                },
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                    "prod".to_string()
                ))),
            }))
        );
        assert_eq!(
            plan.projections,
            vec![Projection::Expression {
                expression: ScalarExpression::LTrim {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                },
                alias: "left_trimmed".to_string(),
            }]
        );
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::RTrim { .. }),
                direction: OrderDirection::Ascending,
            }]
        ));
    }

    #[test]
    fn compiles_gql_string_function_aliases() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE lower(service.name) CONTAINS 'api' \
             RETURN upper(service.tier) AS tier_upper, \
                    btrim(service.name) AS name_trimmed \
             ORDER BY btrim(service.name)",
        )
        .expect("GQL string function aliases should compile");

        assert!(matches!(
            &plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::ToLower { .. },
                operator: ComparisonOperator::Contains,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(value))),
            })) if value == "api"
        ));
        assert!(matches!(
            plan.projections.as_slice(),
            [
                Projection::Expression {
                    expression: ScalarExpression::ToUpper { .. },
                    alias: tier_alias,
                },
                Projection::Expression {
                    expression: ScalarExpression::Trim { .. },
                    alias: trim_alias,
                },
            ] if tier_alias == "tier_upper" && trim_alias == "name_trimmed"
        ));
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Trim { .. }),
                direction: OrderDirection::Ascending,
            }]
        ));
    }

    #[test]
    fn rejects_gql_string_aliases_with_unsupported_arity() {
        let error = compile_cypher(
            "MATCH (service:Service) \
             RETURN btrim(service.name, '-') AS name_trimmed",
        )
        .expect_err("btrim() should require one argument");

        assert!(
            error
                .to_string()
                .contains("btrim() requires exactly one argument"),
            "{error}"
        );
    }

    #[test]
    fn compiles_replace_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE replace(service.name, '-', '') = 'billingapi' \
             RETURN replace(service.team, 'platform', 'core') AS normalized_team \
             ORDER BY replace(service.name, '-', '')",
        )
        .expect("replace scalar expressions should compile");

        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Replace {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    search: Box::new(ScalarExpression::Literal(Literal::String("-".to_string()))),
                    replacement: Box::new(ScalarExpression::Literal(
                        Literal::String(String::new())
                    )),
                },
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                    "billingapi".to_string()
                ))),
            }))
        );
        assert_eq!(
            plan.projections,
            vec![Projection::Expression {
                expression: ScalarExpression::Replace {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "team".to_string(),
                    })),
                    search: Box::new(ScalarExpression::Literal(Literal::String(
                        "platform".to_string()
                    ))),
                    replacement: Box::new(ScalarExpression::Literal(Literal::String(
                        "core".to_string()
                    ))),
                },
                alias: "normalized_team".to_string(),
            }]
        );
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Replace { .. }),
                direction: OrderDirection::Ascending,
            }]
        ));
    }

    #[test]
    fn compiles_character_length_and_substring_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE size(service.name) > 10 \
             RETURN substring(service.name, 0, 7) AS prefix, \
                    char_length(service.tier) AS tier_length \
             ORDER BY character_length(service.name)",
        )
        .expect("string length and substring scalar expressions should compile");

        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::CharacterLength {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                },
                operator: ComparisonOperator::GreaterThan,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(
                    10
                ))),
            }))
        );
        assert_eq!(
            plan.projections,
            vec![
                Projection::Expression {
                    expression: ScalarExpression::Substring {
                        expression: Box::new(ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "name".to_string(),
                        })),
                        start: Box::new(ScalarExpression::Literal(Literal::Integer(0))),
                        length: Some(Box::new(ScalarExpression::Literal(Literal::Integer(7)))),
                    },
                    alias: "prefix".to_string(),
                },
                Projection::Expression {
                    expression: ScalarExpression::CharacterLength {
                        expression: Box::new(ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "tier".to_string(),
                        })),
                    },
                    alias: "tier_length".to_string(),
                },
            ]
        );
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::CharacterLength { .. }),
                direction: OrderDirection::Ascending,
            }]
        ));
    }

    #[test]
    fn compiles_is_empty_string_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE isEmpty(trim(service.tier)) OR NOT isEmpty(service.name) \
             RETURN service.name",
        )
        .expect("isEmpty predicates should compile");

        assert!(plan.predicates.is_empty());
        assert!(matches!(
            &plan.predicate,
            Some(PredicateExpression::Or { left, right })
                if matches!(
                    left.as_ref(),
                    PredicateExpression::ScalarComparison(ScalarPredicate {
                        lhs: ScalarExpression::CharacterLength { expression },
                        operator: ComparisonOperator::Equal,
                        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
                    }) if matches!(expression.as_ref(), ScalarExpression::Trim { .. })
                ) && matches!(
                    right.as_ref(),
                    PredicateExpression::Not { expression }
                        if matches!(
                            expression.as_ref(),
                            PredicateExpression::ScalarComparison(ScalarPredicate {
                                lhs: ScalarExpression::CharacterLength { expression },
                                operator: ComparisonOperator::Equal,
                                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
                            }) if matches!(
                                expression.as_ref(),
                                ScalarExpression::Property(PropertyRef { property, .. }) if property == "name"
                            )
                        )
                )
        ));
    }

    #[test]
    fn rejects_is_empty_with_unsupported_arity() {
        let error = compile_cypher(
            "MATCH (service:Service) \
             WHERE isEmpty(service.name, service.tier) \
             RETURN service.name",
        )
        .expect_err("isEmpty() requires one argument");

        assert!(
            error
                .to_string()
                .contains("isEmpty() supports exactly one scalar string argument"),
            "{error}"
        );
    }

    #[test]
    fn rejects_substring_with_unsupported_arity() {
        let error = compile_cypher(
            "MATCH (service:Service) \
             RETURN substring(service.name) AS prefix",
        )
        .expect_err("substring() requires a start argument");

        assert!(
            error
                .to_string()
                .contains("substring() requires exactly two or three arguments"),
            "{error}"
        );
    }

    #[test]
    fn compiles_left_right_and_reverse_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE left(service.name, 7) = 'billing' \
             RETURN right(service.name, 3) AS suffix, \
                    reverse(service.tier) AS reversed_tier \
             ORDER BY reverse(service.name)",
        )
        .expect("left, right, and reverse scalar expressions should compile");

        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Left {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    count: Box::new(ScalarExpression::Literal(Literal::Integer(7))),
                },
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                    "billing".to_string()
                ))),
            }))
        );
        assert_eq!(
            plan.projections,
            vec![
                Projection::Expression {
                    expression: ScalarExpression::Right {
                        expression: Box::new(ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "name".to_string(),
                        })),
                        count: Box::new(ScalarExpression::Literal(Literal::Integer(3))),
                    },
                    alias: "suffix".to_string(),
                },
                Projection::Expression {
                    expression: ScalarExpression::Reverse {
                        expression: Box::new(ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "tier".to_string(),
                        })),
                    },
                    alias: "reversed_tier".to_string(),
                },
            ]
        );
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Reverse { .. }),
                direction: OrderDirection::Ascending,
            }]
        ));
    }

    #[test]
    fn rejects_left_with_unsupported_arity() {
        let error = compile_cypher(
            "MATCH (service:Service) \
             RETURN left(service.name) AS prefix",
        )
        .expect_err("left() requires a count argument");

        assert!(
            error
                .to_string()
                .contains("left() requires exactly two arguments"),
            "{error}"
        );
    }

    #[test]
    fn compiles_numeric_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE abs(service.risk - 1.0) < 0.2 \
             RETURN ceil(service.risk) AS risk_ceiling, \
                    floor(service.risk) AS risk_floor, \
                    round(service.risk, 1) AS risk_rounded \
             ORDER BY round(service.risk)",
        )
        .expect("numeric scalar functions should compile");

        assert!(matches!(
            &plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Abs { expression },
                operator: ComparisonOperator::LessThan,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Float(_))),
            })) if matches!(
                expression.as_ref(),
                ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Subtract,
                    ..
                }
            )
        ));
        assert_eq!(
            plan.projections,
            vec![
                Projection::Expression {
                    expression: ScalarExpression::Ceil {
                        expression: Box::new(ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "risk".to_string(),
                        })),
                    },
                    alias: "risk_ceiling".to_string(),
                },
                Projection::Expression {
                    expression: ScalarExpression::Floor {
                        expression: Box::new(ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "risk".to_string(),
                        })),
                    },
                    alias: "risk_floor".to_string(),
                },
                Projection::Expression {
                    expression: ScalarExpression::Round {
                        expression: Box::new(ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "risk".to_string(),
                        })),
                        places: Some(Box::new(ScalarExpression::Literal(Literal::Integer(1)))),
                    },
                    alias: "risk_rounded".to_string(),
                },
            ]
        );
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Round { places: None, .. }),
                direction: OrderDirection::Ascending,
            }]
        ));
    }

    #[test]
    fn compiles_more_numeric_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE sqrt(service.risk) < 1.0 AND sign(service.risk - 0.5) = 1 \
             RETURN sqrt(service.risk) AS risk_root, \
                    sign(service.risk - 0.5) AS risk_sign, \
                    exp(service.risk) AS risk_exp, \
                    log(service.risk) AS risk_log, \
                    log10(service.risk) AS risk_log10 \
             ORDER BY log(service.risk)",
        )
        .expect("additional numeric scalar functions should compile");

        assert!(matches!(
            &plan.predicate,
            Some(PredicateExpression::And { left, right })
                if matches!(
                    left.as_ref(),
                    PredicateExpression::ScalarComparison(ScalarPredicate {
                        lhs: ScalarExpression::Sqrt { .. },
                        operator: ComparisonOperator::LessThan,
                        ..
                    })
                ) && matches!(
                    right.as_ref(),
                    PredicateExpression::ScalarComparison(ScalarPredicate {
                        lhs: ScalarExpression::Sign { expression },
                        operator: ComparisonOperator::Equal,
                        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(1))),
                    }) if matches!(
                        expression.as_ref(),
                        ScalarExpression::Arithmetic {
                            operator: ArithmeticOperator::Subtract,
                            ..
                        }
                    )
                )
        ));
        assert!(matches!(
            plan.projections.as_slice(),
            [
                Projection::Expression {
                    expression: ScalarExpression::Sqrt { .. },
                    alias
                },
                Projection::Expression {
                    expression: ScalarExpression::Sign { .. },
                    ..
                },
                Projection::Expression {
                    expression: ScalarExpression::Exp { .. },
                    ..
                },
                Projection::Expression {
                    expression: ScalarExpression::Log { .. },
                    ..
                },
                Projection::Expression {
                    expression: ScalarExpression::Log10 { .. },
                    ..
                },
            ] if alias == "risk_root"
        ));
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Log { .. }),
                direction: OrderDirection::Ascending,
            }]
        ));
    }

    #[test]
    fn rejects_round_with_unsupported_arity() {
        let error = compile_cypher(
            "MATCH (service:Service) \
             RETURN round(service.risk, 1, 2) AS rounded",
        )
        .expect_err("round() supports only optional places");

        assert!(
            error
                .to_string()
                .contains("round() requires exactly one or two arguments"),
            "{error}"
        );
    }

    #[test]
    fn rejects_more_numeric_scalars_with_unsupported_arity() {
        for cypher in [
            "MATCH (service:Service) RETURN sqrt() AS value",
            "MATCH (service:Service) RETURN sign(service.risk, 1) AS value",
            "MATCH (service:Service) RETURN exp(service.risk, 1) AS value",
            "MATCH (service:Service) RETURN log(service.risk, 10) AS value",
            "MATCH (service:Service) RETURN log10(service.risk, 10) AS value",
        ] {
            let error = compile_cypher(cypher).expect_err("wrong arity should be rejected");
            assert!(
                error.to_string().contains("requires exactly one argument"),
                "{error}"
            );
        }
    }

    #[test]
    fn compiles_trigonometric_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE sin(service.risk) >= 0 AND atan2(service.risk, 1.0) < 1.0 \
             RETURN sin(service.risk) AS risk_sin, \
                    cos(service.risk) AS risk_cos, \
                    tan(service.risk) AS risk_tan, \
                    cot(service.risk) AS risk_cot, \
                    asin(0.5) AS half_asin, \
                    acos(1.0) AS one_acos, \
                    atan(service.risk) AS risk_atan, \
                    atan2(service.risk, 1.0) AS risk_atan2, \
                    degrees(service.risk) AS risk_degrees, \
                    radians(180.0) AS pi_radians \
             ORDER BY radians(degrees(service.risk))",
        )
        .expect("trigonometric scalar functions should compile");

        assert!(matches!(
            &plan.predicate,
            Some(PredicateExpression::And { left, right })
                if matches!(
                    left.as_ref(),
                    PredicateExpression::ScalarComparison(ScalarPredicate {
                        lhs: ScalarExpression::Sin { .. },
                        operator: ComparisonOperator::GreaterThanOrEqual,
                        ..
                    })
                ) && matches!(
                    right.as_ref(),
                    PredicateExpression::ScalarComparison(ScalarPredicate {
                        lhs: ScalarExpression::Atan2 { .. },
                        operator: ComparisonOperator::LessThan,
                        ..
                    })
                )
        ));
        assert!(matches!(
            plan.projections.as_slice(),
            [
                Projection::Expression {
                    expression: ScalarExpression::Sin { .. },
                    alias
                },
                Projection::Expression {
                    expression: ScalarExpression::Cos { .. },
                    ..
                },
                Projection::Expression {
                    expression: ScalarExpression::Tan { .. },
                    ..
                },
                Projection::Expression {
                    expression: ScalarExpression::Cot { .. },
                    ..
                },
                Projection::Expression {
                    expression: ScalarExpression::Asin { .. },
                    ..
                },
                Projection::Expression {
                    expression: ScalarExpression::Acos { .. },
                    ..
                },
                Projection::Expression {
                    expression: ScalarExpression::Atan { .. },
                    ..
                },
                Projection::Expression {
                    expression: ScalarExpression::Atan2 { .. },
                    ..
                },
                Projection::Expression {
                    expression: ScalarExpression::Degrees { .. },
                    ..
                },
                Projection::Expression {
                    expression: ScalarExpression::Radians { .. },
                    ..
                },
            ] if alias == "risk_sin"
        ));
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Radians { .. }),
                direction: OrderDirection::Ascending,
            }]
        ));
    }

    #[test]
    fn rejects_trigonometric_scalars_with_unsupported_arity() {
        for cypher in [
            "MATCH (service:Service) RETURN sin() AS value",
            "MATCH (service:Service) RETURN cos(service.risk, 1) AS value",
            "MATCH (service:Service) RETURN tan() AS value",
            "MATCH (service:Service) RETURN cot(service.risk, 1) AS value",
            "MATCH (service:Service) RETURN asin() AS value",
            "MATCH (service:Service) RETURN acos(service.risk, 1) AS value",
            "MATCH (service:Service) RETURN atan() AS value",
            "MATCH (service:Service) RETURN degrees(service.risk, 1) AS value",
            "MATCH (service:Service) RETURN radians() AS value",
        ] {
            let error = compile_cypher(cypher).expect_err("wrong arity should be rejected");
            assert!(
                error.to_string().contains("requires exactly one argument"),
                "{error}"
            );
        }

        for cypher in [
            "MATCH (service:Service) RETURN atan2(service.risk) AS value",
            "MATCH (service:Service) RETURN atan2(service.risk, 1, 2) AS value",
        ] {
            let error = compile_cypher(cypher).expect_err("wrong arity should be rejected");
            assert!(
                error.to_string().contains("requires exactly two arguments"),
                "{error}"
            );
        }
    }

    #[test]
    fn compiles_math_constant_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE service.risk < pi() \
             RETURN pi() AS pi_value, e() AS e_value, sin(pi()) AS zeroish \
             ORDER BY e()",
        )
        .expect("math constants should compile");

        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                operator: ComparisonOperator::LessThan,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Float(
                    ordered_float::OrderedFloat(std::f64::consts::PI),
                ))),
            }))
        );
        assert!(matches!(
            plan.projections.as_slice(),
            [
                Projection::Expression {
                    expression: ScalarExpression::Literal(Literal::Float(pi)),
                    alias
                },
                Projection::Expression {
                    expression: ScalarExpression::Literal(Literal::Float(e)),
                    ..
                },
                Projection::Expression {
                    expression: ScalarExpression::Sin { expression },
                    ..
                },
            ] if *pi == ordered_float::OrderedFloat(std::f64::consts::PI)
                && *e == ordered_float::OrderedFloat(std::f64::consts::E)
                && alias == "pi_value"
                && matches!(expression.as_ref(), ScalarExpression::Literal(Literal::Float(value))
                    if *value == ordered_float::OrderedFloat(std::f64::consts::PI))
        ));
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Literal(Literal::Float(e))),
                direction: OrderDirection::Ascending,
            }] if *e == ordered_float::OrderedFloat(std::f64::consts::E)
        ));
    }

    #[test]
    fn rejects_math_constants_with_arguments() {
        for cypher in [
            "MATCH (service:Service) RETURN pi(1) AS value",
            "MATCH (service:Service) RETURN e(service.risk) AS value",
        ] {
            let error = compile_cypher(cypher).expect_err("math constants take no arguments");
            assert!(
                error
                    .to_string()
                    .contains("requires exactly zero arguments"),
                "{error}"
            );
        }
    }

    #[test]
    fn compiles_haversin_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE haversin(service.risk) < 0.1 \
             RETURN haversin(0.0) AS zero_haversin \
             ORDER BY haversin(service.risk)",
        )
        .expect("haversin() should compile");

        assert!(matches!(
            &plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Divide,
                    left,
                    right,
                },
                operator: ComparisonOperator::LessThan,
                ..
            })) if matches!(
                left.as_ref(),
                ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Subtract,
                    ..
                }
            ) && matches!(
                right.as_ref(),
                ScalarExpression::Literal(Literal::Integer(2))
            )
        ));
        assert!(matches!(
            plan.projections.as_slice(),
            [Projection::Expression {
                expression: ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Divide,
                    ..
                },
                alias
            }] if alias == "zero_haversin"
        ));
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Divide,
                    ..
                }),
                direction: OrderDirection::Ascending,
            }]
        ));
    }

    #[test]
    fn rejects_haversin_with_unsupported_arity() {
        for cypher in [
            "MATCH (service:Service) RETURN haversin() AS value",
            "MATCH (service:Service) RETURN haversin(service.risk, 1) AS value",
        ] {
            let error = compile_cypher(cypher).expect_err("wrong arity should be rejected");
            assert!(
                error.to_string().contains("requires exactly one argument"),
                "{error}"
            );
        }
    }

    #[test]
    fn compiles_scalar_cast_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE toInteger(service.id) = 10 \
             RETURN toFloat(service.risk) AS risk_float, \
                    toBoolean(service.active) AS active_bool \
             ORDER BY toInteger(service.id)",
        )
        .expect("scalar cast expressions should compile");

        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::ToInteger {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "id".to_string(),
                    })),
                },
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(
                    10
                ))),
            }))
        );
        assert_eq!(
            plan.projections,
            vec![
                Projection::Expression {
                    expression: ScalarExpression::ToFloat {
                        expression: Box::new(ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "risk".to_string(),
                        })),
                    },
                    alias: "risk_float".to_string(),
                },
                Projection::Expression {
                    expression: ScalarExpression::ToBoolean {
                        expression: Box::new(ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "active".to_string(),
                        })),
                    },
                    alias: "active_bool".to_string(),
                },
            ]
        );
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::ToInteger { .. }),
                direction: OrderDirection::Ascending,
            }]
        ));
    }

    #[test]
    fn compiles_nullable_scalar_cast_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE toIntegerOrNull(service.id) = 10 \
             RETURN toStringOrNull(service.id) AS id_text, \
                    toFloatOrNull(service.risk) AS risk_float, \
                    toBooleanOrNull(service.active) AS active_bool \
             ORDER BY toIntegerOrNull(service.id)",
        )
        .expect("nullable scalar cast expressions should compile");

        assert!(matches!(
            &plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::ToIntegerOrNull { .. },
                operator: ComparisonOperator::Equal,
                ..
            }))
        ));
        assert!(matches!(
            plan.projections.as_slice(),
            [
                Projection::Expression {
                    expression: ScalarExpression::ToStringOrNull { .. },
                    alias
                },
                Projection::Expression {
                    expression: ScalarExpression::ToFloatOrNull { .. },
                    ..
                },
                Projection::Expression {
                    expression: ScalarExpression::ToBooleanOrNull { .. },
                    ..
                },
            ] if alias == "id_text"
        ));
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::ToIntegerOrNull { .. }),
                direction: OrderDirection::Ascending,
            }]
        ));
    }

    #[test]
    fn rejects_nullable_scalar_casts_with_unsupported_arity() {
        for cypher in [
            "MATCH (service:Service) RETURN toStringOrNull() AS value",
            "MATCH (service:Service) RETURN toIntegerOrNull(service.id, 10) AS value",
            "MATCH (service:Service) RETURN toFloatOrNull() AS value",
            "MATCH (service:Service) RETURN toBooleanOrNull(service.active, false) AS value",
        ] {
            let error = compile_cypher(cypher).expect_err("wrong arity should be rejected");
            assert!(
                error.to_string().contains("requires exactly one argument"),
                "{error}"
            );
        }
    }

    #[test]
    fn compiles_arithmetic_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE service.risk * 100 >= 50 \
             RETURN service.risk * 100 + 1 AS risk_points, \
                    service.risk ^ 2 AS risk_squared \
             ORDER BY service.id % 20",
        )
        .expect("arithmetic scalar expressions should compile");

        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Multiply,
                    left: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    })),
                    right: Box::new(ScalarExpression::Literal(Literal::Integer(100))),
                },
                operator: ComparisonOperator::GreaterThanOrEqual,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(
                    50
                ))),
            }))
        );
        assert_eq!(
            plan.projections,
            vec![
                Projection::Expression {
                    expression: ScalarExpression::Arithmetic {
                        operator: ArithmeticOperator::Add,
                        left: Box::new(ScalarExpression::Arithmetic {
                            operator: ArithmeticOperator::Multiply,
                            left: Box::new(ScalarExpression::Property(PropertyRef {
                                variable: "service".to_string(),
                                property: "risk".to_string(),
                            })),
                            right: Box::new(ScalarExpression::Literal(Literal::Integer(100))),
                        }),
                        right: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
                    },
                    alias: "risk_points".to_string(),
                },
                Projection::Expression {
                    expression: ScalarExpression::Arithmetic {
                        operator: ArithmeticOperator::Power,
                        left: Box::new(ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "risk".to_string(),
                        })),
                        right: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
                    },
                    alias: "risk_squared".to_string(),
                },
            ]
        );
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Modulo,
                    ..
                }),
                direction: OrderDirection::Ascending,
            }]
        ));
    }

    #[test]
    fn compiles_unary_negation_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE -service.risk < -0.8 \
             RETURN -service.risk AS inverse_risk, \
                    -(service.risk * 100) AS inverse_points \
             ORDER BY -service.risk",
        )
        .expect("unary negation scalar expressions should compile");

        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Negate {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    })),
                },
                operator: ComparisonOperator::LessThan,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Float(
                    OrderedFloat(-0.8)
                ))),
            }))
        );
        assert!(matches!(
            plan.projections.as_slice(),
            [
                Projection::Expression {
                    expression: ScalarExpression::Negate { expression },
                    alias,
                },
                Projection::Expression {
                    expression: ScalarExpression::Negate {
                        expression: nested
                    },
                    alias: nested_alias,
                },
            ] if alias == "inverse_risk"
                && matches!(expression.as_ref(), ScalarExpression::Property(_))
                && nested_alias == "inverse_points"
                && matches!(nested.as_ref(), ScalarExpression::Arithmetic { .. })
        ));
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Negate { .. }),
                direction: OrderDirection::Ascending,
            }]
        ));
    }

    #[test]
    fn compiles_searched_case_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN CASE \
                      WHEN service.risk >= 0.75 THEN 'high' \
                      WHEN service.active AND service.tier = 'prod' THEN 'watch' \
                      ELSE 'normal' \
                    END AS risk_band \
             ORDER BY CASE WHEN service.active THEN 0 ELSE 1 END",
        )
        .expect("searched CASE scalar expressions should compile");

        let [
            Projection::Expression {
                expression:
                    ScalarExpression::Case {
                        alternatives,
                        else_expression,
                    },
                alias,
            },
        ] = plan.projections.as_slice()
        else {
            panic!("expected CASE expression projection");
        };
        assert_eq!(alias, "risk_band");
        let [high_alternative, watch_alternative] = alternatives.as_slice() else {
            panic!("expected two CASE alternatives");
        };
        assert!(matches!(
            &high_alternative.when,
            PredicateExpression::Comparison(PropertyPredicate {
                property: PropertyRef { variable, property },
                operator: ComparisonOperator::GreaterThanOrEqual,
                rhs: PredicateRhs::Literal(Literal::Float(_)),
            }) if variable == "service" && property == "risk"
        ));
        assert_eq!(
            high_alternative.then,
            ScalarExpression::Literal(Literal::String("high".to_string()))
        );
        assert!(matches!(
            &watch_alternative.when,
            PredicateExpression::And { .. }
        ));
        assert_eq!(
            else_expression.as_deref(),
            Some(&ScalarExpression::Literal(Literal::String(
                "normal".to_string()
            )))
        );
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Case { .. }),
                direction: OrderDirection::Ascending,
            }]
        ));
    }

    #[test]
    fn compiles_graph_null_checks_inside_searched_case_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[owns:OWNS]->(service) \
             RETURN CASE \
                      WHEN person IS NULL THEN 'unowned' \
                      WHEN id(owns) IS NOT NULL THEN person.name \
                      ELSE 'unknown' \
                    END AS ownership_state \
             ORDER BY CASE WHEN person IS NOT NULL THEN 0 ELSE 1 END",
        )
        .expect("CASE graph null checks should compile");

        let [
            Projection::Expression {
                expression:
                    ScalarExpression::Case {
                        alternatives,
                        else_expression,
                    },
                alias,
            },
        ] = plan.projections.as_slice()
        else {
            panic!("expected CASE expression projection");
        };
        assert_eq!(alias, "ownership_state");
        let [unowned, owned] = alternatives.as_slice() else {
            panic!("expected two CASE alternatives");
        };
        assert_eq!(
            unowned.when,
            PredicateExpression::Presence(PresencePredicate {
                variable: "person".to_string(),
                operator: ComparisonOperator::Equal,
            })
        );
        assert!(matches!(
            &owned.when,
            PredicateExpression::KeyComparison(KeyPredicate {
                variable,
                operator: ComparisonOperator::NotEqual,
                rhs: PredicateRhs::Literal(Literal::Null),
            }) if variable == "owns"
        ));
        assert_eq!(
            else_expression.as_deref(),
            Some(&ScalarExpression::Literal(Literal::String(
                "unknown".to_string()
            )))
        );
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Case {
                    alternatives,
                    ..
                }),
                direction: OrderDirection::Ascending,
            }] if matches!(
                alternatives.as_slice(),
                [ScalarCaseAlternative {
                    when: PredicateExpression::Presence(PresencePredicate {
                        variable,
                        operator: ComparisonOperator::NotEqual,
                    }),
                    ..
                }] if variable == "person"
            )
        ));
    }

    #[test]
    fn compiles_graph_metadata_predicates_inside_searched_case_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             MATCH (person:Person)-[owns:OWNS]->(service) \
             RETURN CASE \
                      WHEN type(owns) = 'OWNS' THEN 'relationship' \
                      WHEN service:Service AND 'Service' IN labels(service) AND 'source' IN keys(owns) THEN 'metadata' \
                      ELSE 'unknown' \
                    END AS state \
             ORDER BY CASE WHEN type(owns) IN ['OWNS'] THEN 0 ELSE 1 END",
        )
        .expect("CASE graph metadata predicates should compile");

        let [
            Projection::Expression {
                expression: ScalarExpression::Case { alternatives, .. },
                ..
            },
        ] = plan.projections.as_slice()
        else {
            panic!("expected CASE expression projection");
        };
        let [relationship, metadata] = alternatives.as_slice() else {
            panic!("expected two CASE alternatives");
        };
        assert_eq!(relationship.when, PredicateExpression::Boolean(true));
        assert!(matches!(metadata.when, PredicateExpression::And { .. }));
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Case {
                    alternatives,
                    ..
                }),
                ..
            }] if matches!(
                alternatives.as_slice(),
                [ScalarCaseAlternative {
                    when: PredicateExpression::Boolean(true),
                    ..
                }]
            )
        ));
    }

    #[test]
    fn compiles_xor_inside_searched_case_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN CASE \
                      WHEN service.tier = 'prod' XOR service.name CONTAINS 'billing' THEN 'xor' \
                      ELSE 'other' \
                    END AS marker",
        )
        .expect("searched CASE XOR predicates should compile");

        let [
            Projection::Expression {
                expression: ScalarExpression::Case { alternatives, .. },
                ..
            },
        ] = plan.projections.as_slice()
        else {
            panic!("expected CASE expression projection");
        };
        assert!(matches!(
            alternatives.as_slice(),
            [ScalarCaseAlternative {
                when: PredicateExpression::Xor { .. },
                ..
            }]
        ));
    }

    #[test]
    fn compiles_is_empty_inside_searched_case_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN CASE \
                      WHEN isEmpty(trim(service.tier)) THEN 'empty' \
                      ELSE 'present' \
                    END AS tier_state",
        )
        .expect("searched CASE isEmpty predicates should compile");

        let [
            Projection::Expression {
                expression: ScalarExpression::Case { alternatives, .. },
                ..
            },
        ] = plan.projections.as_slice()
        else {
            panic!("expected CASE expression projection");
        };
        assert!(matches!(
            alternatives.as_slice(),
            [ScalarCaseAlternative {
                when: PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::CharacterLength { expression },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
                }),
                ..
            }] if matches!(expression.as_ref(), ScalarExpression::Trim { .. })
        ));
    }

    #[test]
    fn compiles_generic_case_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN CASE service.tier WHEN 'prod' THEN 'production' ELSE 'other' END AS tier",
        )
        .expect("generic CASE scalar expressions should compile");

        let [
            Projection::Expression {
                expression:
                    ScalarExpression::Case {
                        alternatives,
                        else_expression,
                    },
                alias,
            },
        ] = plan.projections.as_slice()
        else {
            panic!("expected CASE expression projection");
        };
        assert_eq!(alias, "tier");
        let [production_alternative] = alternatives.as_slice() else {
            panic!("expected one CASE alternative");
        };
        assert_eq!(
            production_alternative.when,
            PredicateExpression::Comparison(PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            })
        );
        assert_eq!(
            production_alternative.then,
            ScalarExpression::Literal(Literal::String("production".to_string()))
        );
        assert_eq!(
            else_expression.as_deref(),
            Some(&ScalarExpression::Literal(Literal::String(
                "other".to_string()
            )))
        );
    }

    #[test]
    fn compiles_scalar_null_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE coalesce(service.tier, null) IS NOT NULL \
             RETURN service.name AS service",
        )
        .expect("scalar null predicate should compile");

        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Coalesce {
                    expressions: vec![
                        ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "tier".to_string(),
                        }),
                        ScalarExpression::Literal(Literal::Null),
                    ],
                },
                operator: ComparisonOperator::NotEqual,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null)),
            }))
        );
    }

    #[test]
    fn compiles_coalesce_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE coalesce(service.tier, 'unassigned') = 'prod' \
             RETURN service.name AS service",
        )
        .expect("coalesce predicate should compile");

        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Coalesce {
                    expressions: vec![
                        ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "tier".to_string(),
                        }),
                        ScalarExpression::Literal(Literal::String("unassigned".to_string())),
                    ],
                },
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                    "prod".to_string()
                ))),
            }))
        );
    }

    #[test]
    fn compiles_reversed_coalesce_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE 'prod' = coalesce(service.tier, 'unassigned') \
             RETURN service.name AS service",
        )
        .expect("reversed coalesce predicate should compile");

        assert!(matches!(
            plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                operator: ComparisonOperator::Equal,
                ..
            }))
        ));
    }

    #[test]
    fn compiles_coalesce_in_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE coalesce(service.tier, 'unassigned') IN ['prod', 'dev'] \
             RETURN service.name AS service",
        )
        .expect("coalesce IN predicate should compile");

        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Coalesce {
                    expressions: vec![
                        ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "tier".to_string(),
                        }),
                        ScalarExpression::Literal(Literal::String("unassigned".to_string())),
                    ],
                },
                operator: ComparisonOperator::In,
                rhs: ScalarPredicateRhs::List(vec![
                    Literal::String("prod".to_string()),
                    Literal::String("dev".to_string()),
                ]),
            }))
        );
    }

    #[test]
    fn rejects_invalid_coalesce_projections() {
        for (cypher, expected) in [
            (
                "MATCH (service:Service) RETURN coalesce(service.team) AS owner_team",
                "at least two arguments",
            ),
            (
                "MATCH (service:Service) RETURN coalesce(id(service), 'unknown') AS owner_team",
                "scalar function 'id'",
            ),
        ] {
            let error = compile_cypher(cypher).expect_err("query should be rejected");
            assert!(
                error.to_string().contains(expected),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn rejects_ambiguous_literal_list_projections() {
        for (cypher, expected) in [
            (
                "MATCH (service:Service) RETURN [] AS values",
                "at least one element",
            ),
            (
                "MATCH (service:Service) RETURN [null] AS values",
                "at least one non-null element",
            ),
            (
                "MATCH (service:Service) RETURN [1, 'prod'] AS values",
                "all non-null elements to have the same type",
            ),
        ] {
            let error = compile_cypher(cypher).expect_err("query should be rejected");
            assert!(
                error.to_string().contains(expected),
                "unexpected error: {error}"
            );
        }
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
             WHERE service.tier IN ['prod', null, 'dev'] \
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
                    Literal::Null,
                    Literal::String("dev".to_string()),
                ]),
            }]
        );
    }

    #[test]
    fn compiles_bound_cypher_parameters() {
        let parameters = BTreeMap::from([
            (
                "tier".to_string(),
                CypherParameterValue::Literal(Literal::String("prod".to_string())),
            ),
            (
                "ids".to_string(),
                CypherParameterValue::List(vec![Literal::Integer(10), Literal::Integer(40)]),
            ),
            (
                "limit".to_string(),
                CypherParameterValue::Literal(Literal::Integer(2)),
            ),
        ]);
        let plan = compile_cypher_with_parameters(
            "MATCH (service:Service {tier: $tier}) \
             WHERE service.id IN $ids \
             RETURN service.name \
             LIMIT $limit",
            &parameters,
        )
        .expect("parameterized query should compile");

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
                    operator: ComparisonOperator::In,
                    rhs: PredicateRhs::List(vec![Literal::Integer(10), Literal::Integer(40)]),
                },
            ]
        );
        assert_eq!(plan.limit, Some(2));
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
    fn compiles_dynamic_string_predicate_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE service.name STARTS WITH left(service.name, 4) \
                AND service.name ENDS WITH right(service.name, 3) \
                AND service.name CONTAINS substring(service.name, 1, 3) \
             RETURN service.name",
        )
        .expect("dynamic string predicates should compile");

        assert!(plan.predicates.is_empty());
        assert!(matches!(
            &plan.predicate,
            Some(PredicateExpression::And { left, right })
                if matches!(
                    left.as_ref(),
                    PredicateExpression::And { left, right }
                        if matches!(
                            left.as_ref(),
                            PredicateExpression::ScalarComparison(ScalarPredicate {
                                lhs: ScalarExpression::Property(PropertyRef { property, .. }),
                                operator: ComparisonOperator::StartsWith,
                                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Left { .. }),
                            }) if property == "name"
                        ) && matches!(
                            right.as_ref(),
                            PredicateExpression::ScalarComparison(ScalarPredicate {
                                operator: ComparisonOperator::EndsWith,
                                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Right { .. }),
                                ..
                            })
                        )
                ) && matches!(
                    right.as_ref(),
                    PredicateExpression::ScalarComparison(ScalarPredicate {
                        operator: ComparisonOperator::Contains,
                        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Substring { .. }),
                        ..
                    })
                )
        ));
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
    fn compiles_literal_only_predicates() {
        for (cypher, expected) in [
            (
                "MATCH (service:Service) WHERE 1 = 1 RETURN service.name",
                true,
            ),
            (
                "MATCH (service:Service) WHERE 5 > 3 RETURN service.name",
                true,
            ),
            (
                "MATCH (service:Service) WHERE 1 = 1.0 RETURN service.name",
                true,
            ),
            (
                "MATCH (service:Service) WHERE 'prod' IN ['dev', 'prod', null] RETURN service.name",
                true,
            ),
            (
                "MATCH (service:Service) WHERE 'stage' IN ['dev', 'prod'] RETURN service.name",
                false,
            ),
        ] {
            let plan = compile_cypher(cypher).expect("literal-only predicate should compile");
            assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(expected)));
        }

        let parameters = BTreeMap::from([(
            "enabled".to_string(),
            CypherParameterValue::Literal(Literal::Boolean(true)),
        )]);
        let plan = compile_cypher_with_parameters(
            "MATCH (service:Service) WHERE $enabled = true RETURN service.name",
            &parameters,
        )
        .expect("parameterized literal-only predicate should compile");
        assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
    }

    #[test]
    fn rejects_unsafe_literal_only_predicates() {
        for (cypher, expected) in [
            (
                "MATCH (service:Service) WHERE null = null RETURN service.name",
                "literal-only null comparisons",
            ),
            (
                "MATCH (service:Service) WHERE null IN ['prod'] RETURN service.name",
                "null left-hand side",
            ),
            (
                "MATCH (service:Service) WHERE 'prod' IN ['dev', null] RETURN service.name",
                "null members cannot be folded",
            ),
        ] {
            let error = compile_cypher(cypher).expect_err("query should be rejected");
            assert!(
                error.to_string().contains(expected),
                "unexpected error: {error}"
            );
        }
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
    fn compiles_graph_variable_null_predicates() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE person IS NOT NULL AND owns IS NULL \
             RETURN person.name AS owner",
        )
        .expect("graph variable null predicates should compile");

        assert!(plan.predicates.is_empty());
        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::And {
                left: Box::new(PredicateExpression::Presence(PresencePredicate {
                    variable: "person".to_string(),
                    operator: ComparisonOperator::NotEqual,
                })),
                right: Box::new(PredicateExpression::Presence(PresencePredicate {
                    variable: "owns".to_string(),
                    operator: ComparisonOperator::Equal,
                })),
            })
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
    fn compiles_anchored_optional_match() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN service.name AS service, person.name AS owner",
        )
        .expect("anchored OPTIONAL MATCH should compile");

        assert_eq!(plan.optional_relationships, vec![0]);
        assert_eq!(
            plan.optional_matches,
            vec![OptionalMatchScope {
                relationship_indices: vec![0],
                predicate: None,
            }]
        );
        assert_eq!(
            plan.nodes,
            vec![
                NodePattern {
                    variable: "service".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "person".to_string(),
                    label: "Person".to_string(),
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
    }

    #[test]
    fn compiles_optional_match_local_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person {active: true})-[owns:OWNS {source: 'pagerduty'}]->(service) \
             WHERE person.team = service.team AND id(owns) > 10 \
             RETURN service.name AS service, person.name AS owner",
        )
        .expect("OPTIONAL MATCH predicates should compile");

        assert_eq!(plan.optional_relationships, vec![0]);
        assert_eq!(plan.predicates, Vec::new());
        assert_eq!(plan.optional_matches.len(), 1);
        let optional_match = plan
            .optional_matches
            .first()
            .expect("optional match scope should be present");
        assert_eq!(optional_match.relationship_indices, vec![0]);
        assert!(matches!(
            &optional_match.predicate,
            Some(PredicateExpression::And { .. })
        ));
    }

    #[test]
    fn rejects_unsupported_optional_match_shapes() {
        assert_unsupported("OPTIONAL MATCH (service:Service) RETURN service.name");
        assert_unsupported(
            "MATCH (service:Service) OPTIONAL MATCH (service)-[:DEPENDS_ON]->(target:Service)-[:DEPENDS_ON]->(next:Service) WHERE next.tier = 'prod' RETURN service.name",
        );
        assert_unsupported(
            "MATCH (service:Service) OPTIONAL MATCH (service)-[:DEPENDS_ON]-(target:Service) WHERE target.tier = 'prod' RETURN service.name",
        );
        assert_unsupported(
            "MATCH (service:Service) OPTIONAL MATCH (service)-[:DEPENDS_ON]->(target:Service) MATCH (target)-[:DEPENDS_ON]->(next:Service) RETURN next.name",
        );
    }

    #[test]
    fn rejects_non_transparent_with_boundaries() {
        assert_unsupported("MATCH (service:Service) WITH DISTINCT service RETURN service.name");
        assert_unsupported("MATCH (service:Service) WITH *, service.name AS name RETURN name");
        assert_unsupported(
            "MATCH (service:Service) WITH service LIMIT 1 MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN target.name",
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
        for cypher in [
            "MATCH (a:Service)-[:DEPENDS_ON*]->(b:Service) RETURN a.name",
            "MATCH (a:Service)-[:DEPENDS_ON*0..1]->(b:Service) RETURN a.name",
            "MATCH (a:Service)-[:DEPENDS_ON*1..3]->(b:Service) RETURN a.name",
            "MATCH (a:Service)-[:DEPENDS_ON]->{0,1}(b:Service) RETURN a.name",
            "MATCH (a:Service)-[:DEPENDS_ON]->{1,3}(b:Service) RETURN a.name",
            "MATCH (a:Service)-[:DEPENDS_ON]->{1,}(b:Service) RETURN a.name",
        ] {
            assert_unsupported(cypher);
        }
    }

    #[test]
    fn compiles_exact_one_relationship_ranges_as_single_hop() {
        for cypher in [
            "MATCH (a:Service)-[:DEPENDS_ON*1]->(b:Service) RETURN a.name",
            "MATCH (a:Service)-[:DEPENDS_ON*1..1]->(b:Service) RETURN a.name",
            "MATCH (a:Service)-[:DEPENDS_ON]->{1}(b:Service) RETURN a.name",
            "MATCH (a:Service)-[:DEPENDS_ON]->{1,1}(b:Service) RETURN a.name",
        ] {
            let plan = compile_cypher(cypher).expect("exact-one relationship should compile");

            assert_eq!(
                plan.relationships,
                vec![RelationshipPattern {
                    variable: None,
                    relationship_type: "DEPENDS_ON".to_string(),
                    left: "a".to_string(),
                    direction: Direction::Outgoing,
                    right: "b".to_string(),
                }]
            );
        }
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
    fn compiles_xor_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE service.tier = 'prod' XOR service.tier IS NULL \
             RETURN service.name",
        )
        .expect("XOR predicate should compile");

        assert!(matches!(
            plan.predicate,
            Some(PredicateExpression::Xor { .. })
        ));
    }

    #[test]
    fn compiles_terminal_with_xor_where_alias_predicates() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WITH person.name AS owner, service.tier AS tier \
             WHERE owner STARTS WITH 'Ada' XOR tier = 'prod' \
             RETURN owner, tier",
        )
        .expect("terminal WITH XOR WHERE should compile");

        assert!(matches!(
            plan.post_projection_predicate,
            Some(ProjectionPredicateExpression::Xor { .. })
        ));
    }

    #[test]
    fn rejects_missing_cypher_parameters() {
        let error = compile_cypher(
            "MATCH (service:Service) WHERE service.tier IN $tiers RETURN service.name",
        )
        .expect_err("missing parameter should fail");

        assert!(
            error.to_string().contains("MISSING_PARAMETER"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_cypher_parameter_kind_mismatches() {
        let scalar_for_list = BTreeMap::from([(
            "tiers".to_string(),
            CypherParameterValue::Literal(Literal::String("prod".to_string())),
        )]);
        let error = compile_cypher_with_parameters(
            "MATCH (service:Service) WHERE service.tier IN $tiers RETURN service.name",
            &scalar_for_list,
        )
        .expect_err("scalar parameter should not bind as IN list");
        assert!(
            error.to_string().contains("IN parameter right-hand sides"),
            "unexpected error: {error}"
        );

        let list_for_scalar = BTreeMap::from([(
            "tier".to_string(),
            CypherParameterValue::List(vec![Literal::String("prod".to_string())]),
        )]);
        let error = compile_cypher_with_parameters(
            "MATCH (service:Service) WHERE service.tier = $tier RETURN service.name",
            &list_for_scalar,
        )
        .expect_err("list parameter should not bind as scalar literal");
        assert!(
            error
                .to_string()
                .contains("list parameters can only be used"),
            "unexpected error: {error}"
        );

        let ambiguous_list_projection = BTreeMap::from([(
            "value".to_string(),
            CypherParameterValue::List(vec![Literal::Null]),
        )]);
        let error = compile_cypher_with_parameters(
            "MATCH (service:Service) RETURN $value AS value",
            &ambiguous_list_projection,
        )
        .expect_err("ambiguous list parameter projection should fail");
        assert!(
            error.to_string().contains("at least one non-null element"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn compiles_regex_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) WHERE service.name =~ '^billing.*' RETURN service.name",
        )
        .expect("regex predicate should compile");

        assert_eq!(
            plan.predicates,
            vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                operator: ComparisonOperator::RegexMatch,
                rhs: PredicateRhs::Literal(Literal::String("^billing.*".to_string())),
            }]
        );
    }

    #[test]
    fn compiles_dynamic_regex_predicate_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) WHERE service.name =~ left(service.name, 4) RETURN service.name",
        )
        .expect("dynamic regex predicate should compile");

        assert!(plan.predicates.is_empty());
        assert!(matches!(
            &plan.predicate,
            Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Property(PropertyRef { property, .. }),
                operator: ComparisonOperator::RegexMatch,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Left { .. }),
            })) if property == "name"
        ));
    }

    #[test]
    fn rejects_invalid_literal_regex_predicates() {
        assert_unsupported(
            "MATCH (service:Service) WHERE 'billing-api' =~ '[' RETURN service.name",
        );
    }

    #[test]
    fn rejects_comparisons_without_supported_operands() {
        assert_unsupported("MATCH (service:Service) WHERE service = service RETURN service.name");
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
    fn compiles_anonymous_labeled_node_patterns() {
        let plan = compile_cypher(
            "MATCH (:Service {tier: 'prod'})-[:DEPENDS_ON]->(target:Service) \
             RETURN target.name",
        )
        .expect("anonymous labeled node pattern should compile");

        assert_eq!(plan.nodes.len(), 2);
        let anonymous_node = plan.nodes.first().expect("anonymous node should exist");
        let target_node = plan.nodes.get(1).expect("target node should exist");
        let relationship = plan
            .relationships
            .first()
            .expect("relationship should exist");
        let anonymous_variable = &anonymous_node.variable;
        assert!(anonymous_variable.starts_with("__coral_node_"));
        assert_eq!(anonymous_node.label, "Service");
        assert_eq!(target_node.variable, "target");
        assert_eq!(relationship.left, anonymous_variable.as_str());
        assert_eq!(relationship.right, "target");
        assert_eq!(
            plan.predicates,
            vec![PropertyPredicate {
                property: PropertyRef {
                    variable: anonymous_variable.clone(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            }]
        );
    }

    #[test]
    fn rejects_unlabeled_anonymous_node_patterns() {
        assert_unsupported("MATCH ()-[:DEPENDS_ON]->(target:Service) RETURN target.name");
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
    fn compiles_collect_property_projection() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[:OWNS]->(service:Service) \
             RETURN person.team AS team, collect(DISTINCT service.name) AS services \
             ORDER BY services",
        )
        .expect("collect property query should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "person".to_string(),
                        property: "team".to_string(),
                    },
                    alias: Some("team".to_string()),
                },
                Projection::Aggregate {
                    function: super::AggregateFunction::Collect,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    }),
                    distinct: true,
                    alias: "services".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::ProjectionAlias("services".to_string()),
                direction: OrderDirection::Ascending,
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
        assert_unsupported("MATCH (service:Service) RETURN id(missing)");
        assert_unsupported("MATCH (service:Service) RETURN type(service)");
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

    #[test]
    fn compiles_order_by_count_star_expression() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN service.tier AS tier, count(*) AS services \
             ORDER BY count(*) DESC, tier",
        )
        .expect("count(*) order expression should compile");

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
    }

    #[test]
    fn compiles_order_by_aggregate_expressions() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN count(service) AS services, avg(service.risk) AS average_risk \
             ORDER BY count(service) DESC, avg(service.risk)",
        )
        .expect("aggregate order expressions should compile");

        assert_eq!(
            plan.order_by,
            vec![
                OrderKey {
                    expression: OrderExpression::ProjectionAlias("services".to_string()),
                    direction: OrderDirection::Descending,
                },
                OrderKey {
                    expression: OrderExpression::ProjectionAlias("average_risk".to_string()),
                    direction: OrderDirection::Ascending,
                },
            ]
        );
    }

    #[test]
    fn rejects_unprojected_order_by_aggregate_expressions() {
        assert_unsupported(
            "MATCH (service:Service) \
             RETURN service.tier AS tier \
             ORDER BY count(*)",
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
