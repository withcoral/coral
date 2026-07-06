//! Scoped Cypher subquery lowering helpers split out of `cypher.rs` without
//! changing behavior.

use std::collections::BTreeMap;

use decypher::ast::clause::{ProjectionItem, Return};
use decypher::ast::expr::{
    CollectSubqueryExpression, CountSubqueryExpression, ExistsExpression, ExistsInner, Expression,
    FunctionInvocation, Literal as CypherLiteral,
};
use decypher::ast::pattern::PatternPart;
use decypher::ast::query::{Query, QueryBody, RegularQuery, SinglePartBody, SingleQueryKind};

use super::ComparisonOperator;
use super::CoreError;
use super::CountSubqueryPattern;
use super::CypherCompileContext;
use super::CypherCompileState;
use super::Diagnostic;
use super::ExistsPatternPredicate;
use super::GraphPlan;
use super::Literal;
use super::PredicateCompileMode;
use super::PredicateExpression;
use super::PredicateRhs;
use super::Projection;
use super::PropertyPredicate;
use super::ScalarExpression;
use super::append_predicate_expression;
use super::compile_pattern_part_into;
use super::compile_predicate_expression;
use super::compile_property_ref;
use super::compile_reading_clauses_into;
use super::compile_scalar_expression_in_predicate_mode;
use super::diagnostic_codes;
use super::expression_contains_aggregate;
use super::expression_contains_subquery;
use super::unsupported;
use super::variable_name;

pub(super) fn compile_count_subquery_projection(
    count: &CountSubqueryExpression,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    Ok(Projection::Expression {
        expression: compile_count_subquery_scalar_expression(
            count,
            format!("{path}.expression"),
            Some(plan),
            context,
        )?,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "count".to_string(), variable_name),
    })
}

pub(super) fn compile_collect_subquery_projection(
    collect: &CollectSubqueryExpression,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    Ok(Projection::Expression {
        expression: compile_collect_subquery_scalar_expression(
            collect,
            format!("{path}.expression"),
            Some(plan),
            context,
        )?,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "collect".to_string(), variable_name),
    })
}

pub(super) fn compile_pattern_comprehension_projection(
    comprehension: &decypher::ast::expr::PatternComprehension,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    Ok(Projection::Expression {
        expression: compile_pattern_comprehension_scalar_expression(
            comprehension,
            format!("{path}.expression"),
            Some(plan),
            context,
        )?,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "list".to_string(), variable_name),
    })
}

pub(super) fn compile_exists_predicate(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<PropertyPredicate, CoreError> {
    let path = path.into();
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "exists() supports exactly one variable.property argument",
        ));
    };
    Ok(PropertyPredicate {
        property: compile_property_ref(argument, format!("{path}.arguments[0]"), plan, context)?,
        operator: ComparisonOperator::NotEqual,
        rhs: PredicateRhs::Literal(Literal::Null),
    })
}

pub(super) fn compile_exists_pattern_predicate(
    exists: &ExistsExpression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    match exists.inner.as_ref() {
        ExistsInner::Pattern(pattern, where_clause) => {
            let [part] = pattern.parts.as_slice() else {
                return Err(unsupported(
                    format!("{path}.pattern.parts"),
                    "EXISTS pattern predicates currently support exactly one connected pattern part",
                ));
            };
            match where_clause.as_deref() {
                Some(where_clause) => compile_scoped_pattern_where_predicate(
                    part,
                    where_clause,
                    path,
                    plan,
                    context,
                    "EXISTS pattern predicates",
                    "EXISTS pattern predicates require graph context",
                ),
                None => compile_scoped_pattern_predicate(
                    part,
                    path,
                    plan,
                    context,
                    "EXISTS pattern predicates",
                    "EXISTS pattern predicates require graph context",
                ),
            }
        }
        ExistsInner::RegularQuery(query) => {
            if let Some(compact_query) = context.compact_exists_pattern_query(exists) {
                return compile_compact_exists_pattern_query(compact_query, path, plan, context);
            }
            compile_exists_regular_query_predicate(query, path, plan, context)
        }
    }
}

fn compile_compact_exists_pattern_query(
    source: &str,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let query = decypher::parse(source).map_err(|error| {
        Diagnostic::new(
            diagnostic_codes::CYPHER_PARSE_ERROR,
            path.clone(),
            format!("could not parse compact EXISTS pattern recovery: {error}"),
        )
        .into_core_error()
    })?;
    let regular_query = regular_query_from_single_statement(query, &path)?;
    let fragment_context = CypherCompileContext::from_source_with_parameters_and_graph(
        source,
        context.parameters.clone(),
        context.graph.clone(),
        context.catalog.as_ref(),
        BTreeMap::new(),
    );
    compile_exists_regular_query_predicate(&regular_query, path, plan, &fragment_context)
}

fn regular_query_from_single_statement(
    query: Query,
    path: &str,
) -> Result<RegularQuery, CoreError> {
    let mut statements = query.statements.into_iter();
    let Some(statement) = statements.next() else {
        return Err(CoreError::internal(format!(
            "compact EXISTS pattern recovery at {path} did not produce a query"
        )));
    };
    if statements.next().is_some() {
        return Err(CoreError::internal(format!(
            "compact EXISTS pattern recovery at {path} produced multiple statements"
        )));
    }
    match statement {
        QueryBody::SingleQuery(single_query) => Ok(RegularQuery {
            single_query,
            unions: Vec::new(),
        }),
        QueryBody::Regular(query) => Ok(query),
        _ => Err(CoreError::internal(format!(
            "compact EXISTS pattern recovery at {path} did not produce a data query"
        ))),
    }
}

fn compile_exists_regular_query_predicate(
    query: &RegularQuery,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    compile_regular_query_scoped_pattern(
        query,
        path,
        plan,
        context,
        "EXISTS subqueries",
        "EXISTS subqueries with scoped WHERE predicates currently require an explicit MATCH clause",
        true,
    )
    .map(PredicateExpression::ExistsPattern)
}

pub(super) fn compile_count_subquery_scalar_expression(
    count: &CountSubqueryExpression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let (pattern, distinct_target) = compile_regular_query_count_subquery(
        &count.query,
        format!("{path}.query"),
        plan,
        context,
        "COUNT subqueries",
        "COUNT subqueries require an explicit MATCH clause",
    )?;
    Ok(ScalarExpression::CountSubquery {
        pattern: Box::new(pattern),
        distinct_target: distinct_target.map(Box::new),
    })
}

pub(super) fn compile_collect_subquery_scalar_expression(
    collect: &CollectSubqueryExpression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let (pattern, target, distinct) = compile_regular_query_collect_subquery(
        &collect.query,
        format!("{path}.query"),
        plan,
        context,
        "COLLECT subqueries",
        "COLLECT subqueries require an explicit MATCH clause",
    )?;
    Ok(ScalarExpression::CollectSubquery {
        pattern: Box::new(pattern),
        target: Box::new(target),
        distinct,
    })
}

pub(super) fn compile_collect_subquery_count_scalar_expression(
    collect: &CollectSubqueryExpression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let (pattern, target, distinct) = compile_regular_query_collect_subquery(
        &collect.query,
        format!("{path}.query"),
        plan,
        context,
        "COLLECT subqueries",
        "COLLECT subqueries require an explicit MATCH clause",
    )?;
    Ok(ScalarExpression::CountSubquery {
        pattern: Box::new(pattern),
        distinct_target: distinct.then_some(Box::new(target)),
    })
}

pub(super) fn compile_pattern_comprehension_scalar_expression(
    comprehension: &decypher::ast::expr::PatternComprehension,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let Some(source) = context.pattern_comprehension_source(comprehension) else {
        return Err(unsupported(
            path,
            "pattern comprehensions require recoverable source text",
        ));
    };
    let query = decypher::parse(&source.collect_query_source).map_err(|error| {
        Diagnostic::new(
            diagnostic_codes::CYPHER_PARSE_ERROR,
            path.clone(),
            format!("could not parse pattern comprehension recovery: {error}"),
        )
        .into_core_error()
    })?;
    let regular_query = regular_query_from_single_statement(query, &path)?;
    let fragment_context = CypherCompileContext::from_source_with_parameters_and_graph(
        &source.collect_query_source,
        context.parameters.clone(),
        context.graph.clone(),
        context.catalog.as_ref(),
        BTreeMap::new(),
    );
    let (pattern, target, distinct) = compile_regular_query_collect_subquery(
        &regular_query,
        path.clone(),
        plan,
        &fragment_context,
        "pattern comprehensions",
        "pattern comprehensions require a relationship pattern",
    )?;
    Ok(ScalarExpression::CollectSubquery {
        pattern: Box::new(pattern),
        target: Box::new(target),
        distinct,
    })
}

pub(super) fn compile_pattern_comprehension_count_scalar_expression(
    comprehension: &decypher::ast::expr::PatternComprehension,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let Some(source) = context.pattern_comprehension_source(comprehension) else {
        return Err(unsupported(
            path,
            "pattern comprehensions require recoverable source text",
        ));
    };
    let query = decypher::parse(&source.count_query_source).map_err(|error| {
        Diagnostic::new(
            diagnostic_codes::CYPHER_PARSE_ERROR,
            path.clone(),
            format!("could not parse pattern comprehension count recovery: {error}"),
        )
        .into_core_error()
    })?;
    let regular_query = regular_query_from_single_statement(query, &path)?;
    let fragment_context = CypherCompileContext::from_source_with_parameters_and_graph(
        &source.count_query_source,
        context.parameters.clone(),
        context.graph.clone(),
        context.catalog.as_ref(),
        BTreeMap::new(),
    );
    let (pattern, distinct_target) = compile_regular_query_count_subquery(
        &regular_query,
        path.clone(),
        plan,
        &fragment_context,
        "pattern comprehensions",
        "pattern comprehensions require a relationship pattern",
    )?;
    Ok(ScalarExpression::CountSubquery {
        pattern: Box::new(pattern),
        distinct_target: distinct_target.map(Box::new),
    })
}

#[derive(Debug)]
struct CompiledScopedPlan<'a> {
    plan: GraphPlan,
    state: CypherCompileState,
    delta: ScopedPlanDelta,
    return_clause: Option<&'a Return>,
}

fn compile_regular_query_scoped_pattern(
    query: &RegularQuery,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
    feature_name: &'static str,
    missing_match_message: &'static str,
    allow_distinct_noop_return: bool,
) -> Result<ExistsPatternPredicate, CoreError> {
    let path = path.into();
    let Some(plan) = plan else {
        return Err(unsupported(
            path,
            format!("{feature_name} require graph context"),
        ));
    };
    let scoped = compile_regular_query_scoped_plan(
        query,
        &path,
        plan,
        context,
        feature_name,
        missing_match_message,
        allow_distinct_noop_return,
    )?;
    compile_scoped_plan_delta_pattern(scoped.plan, plan, scoped.delta, path, feature_name)
}

fn compile_regular_query_count_subquery(
    query: &RegularQuery,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
    feature_name: &'static str,
    missing_match_message: &'static str,
) -> Result<(CountSubqueryPattern, Option<ScalarExpression>), CoreError> {
    let path = path.into();
    let Some(plan) = plan else {
        return Err(unsupported(
            path,
            format!("{feature_name} require graph context"),
        ));
    };
    let scoped = compile_regular_query_scoped_plan(
        query,
        &path,
        plan,
        context,
        feature_name,
        missing_match_message,
        true,
    )?;
    let distinct_target = scoped
        .return_clause
        .filter(|return_clause| return_clause.distinct)
        .map(|return_clause| {
            compile_count_subquery_distinct_target(
                return_clause,
                &path,
                feature_name,
                &scoped.plan,
                &scoped.state,
                context,
            )
        })
        .transpose()?;
    let pattern =
        compile_scoped_plan_delta_count_subquery(scoped.plan, scoped.delta, path, feature_name)?;
    Ok((pattern, distinct_target))
}

fn compile_regular_query_collect_subquery(
    query: &RegularQuery,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
    feature_name: &'static str,
    missing_match_message: &'static str,
) -> Result<(CountSubqueryPattern, ScalarExpression, bool), CoreError> {
    let path = path.into();
    let Some(plan) = plan else {
        return Err(unsupported(
            path,
            format!("{feature_name} require graph context"),
        ));
    };
    if !query.unions.is_empty() {
        return Err(unsupported(
            format!("{path}.unions"),
            format!(
                "{feature_name} with UNION require staged subquery planning and are not supported yet"
            ),
        ));
    }
    let SingleQueryKind::SinglePart(single_part) = &query.single_query.kind else {
        return Err(unsupported(
            format!("{path}.single_query"),
            format!(
                "{feature_name} with WITH require staged subquery planning and are not supported yet"
            ),
        ));
    };
    let return_clause = scoped_collect_subquery_return_clause(&single_part.body, &path)?;
    let return_item = validate_scoped_collect_subquery_return(return_clause, &path, feature_name)?;
    let distinct = return_clause.distinct;
    if single_part.reading_clauses.is_empty() {
        return Err(unsupported(format!("{path}.match"), missing_match_message));
    }

    let mut collect_plan = plan.clone();
    collect_plan.predicate = None;
    let mut collect_state = CypherCompileState::default();
    let node_start = collect_plan.nodes.len();
    let relationship_start = collect_plan.relationships.len();
    let predicate_start = collect_plan.predicates.len();
    let optional_relationship_start = collect_plan.optional_relationships.len();
    let optional_match_start = collect_plan.optional_matches.len();
    compile_reading_clauses_into(
        &single_part.reading_clauses,
        format!("{path}.match"),
        &mut collect_plan,
        &mut collect_state,
        context,
    )?;
    if collect_plan.optional_relationships.len() > optional_relationship_start
        || collect_plan.optional_matches.len() > optional_match_start
    {
        return Err(unsupported(
            format!("{path}.match"),
            format!(
                "OPTIONAL MATCH inside {feature_name} requires nullable scoped row-source planning and is not supported yet"
            ),
        ));
    }

    let target = compile_scalar_expression_in_predicate_mode(
        &return_item.expression,
        format!("{path}.return.items[0].expression"),
        PredicateCompileMode::Graph {
            plan: &collect_plan,
            path_state: Some(&collect_state),
        },
        context,
    )?;
    let pattern = compile_scoped_plan_delta_count_subquery(
        collect_plan,
        ScopedPlanDelta {
            nodes_before: node_start,
            relationship_base: relationship_start,
            predicate_offset: predicate_start,
        },
        path,
        feature_name,
    )?;
    Ok((pattern, target, distinct))
}

fn compile_regular_query_scoped_plan<'a>(
    query: &'a RegularQuery,
    path: &str,
    plan: &GraphPlan,
    context: &CypherCompileContext,
    feature_name: &'static str,
    missing_match_message: &'static str,
    allow_distinct_noop_return: bool,
) -> Result<CompiledScopedPlan<'a>, CoreError> {
    let path = path.to_string();
    if !query.unions.is_empty() {
        return Err(unsupported(
            format!("{path}.unions"),
            format!(
                "{feature_name} with UNION require staged subquery planning and are not supported yet"
            ),
        ));
    }
    let SingleQueryKind::SinglePart(single_part) = &query.single_query.kind else {
        return Err(unsupported(
            format!("{path}.single_query"),
            format!(
                "{feature_name} with WITH require staged subquery planning and are not supported yet"
            ),
        ));
    };
    let return_clause = match &single_part.body {
        SinglePartBody::Finish(_) => None,
        SinglePartBody::Return(return_clause) => Some(return_clause),
        SinglePartBody::Updating {
            updating,
            return_clause,
        } if updating.is_empty() && return_clause.is_none() => None,
        SinglePartBody::Updating {
            updating,
            return_clause,
        } if updating.is_empty() && return_clause.is_some() => {
            let Some(return_clause) = return_clause else {
                unreachable!("return_clause.is_some() was checked above");
            };
            Some(return_clause)
        }
        SinglePartBody::Updating { .. } => {
            return Err(unsupported(
                format!("{path}.updating"),
                "write clauses are not supported by Coral virtual graphs",
            ));
        }
    };
    if single_part.reading_clauses.is_empty() {
        return Err(unsupported(format!("{path}.match"), missing_match_message));
    }
    let mut exists_plan = plan.clone();
    exists_plan.predicate = None;
    let mut exists_state = CypherCompileState::default();
    let node_start = exists_plan.nodes.len();
    let relationship_start = exists_plan.relationships.len();
    let predicate_start = exists_plan.predicates.len();
    let optional_relationship_start = exists_plan.optional_relationships.len();
    let optional_match_start = exists_plan.optional_matches.len();
    compile_reading_clauses_into(
        &single_part.reading_clauses,
        format!("{path}.match"),
        &mut exists_plan,
        &mut exists_state,
        context,
    )?;
    if exists_plan.optional_relationships.len() > optional_relationship_start
        || exists_plan.optional_matches.len() > optional_match_start
    {
        return Err(unsupported(
            format!("{path}.match"),
            format!(
                "OPTIONAL MATCH inside {feature_name} requires nullable scoped predicate planning and is not supported yet"
            ),
        ));
    }
    if let Some(return_clause) = return_clause {
        validate_scoped_subquery_noop_return(
            return_clause,
            &path,
            feature_name,
            allow_distinct_noop_return,
            &exists_plan,
            &exists_state,
            context,
        )?;
    }
    Ok(CompiledScopedPlan {
        plan: exists_plan,
        state: exists_state,
        delta: ScopedPlanDelta {
            nodes_before: node_start,
            relationship_base: relationship_start,
            predicate_offset: predicate_start,
        },
        return_clause,
    })
}

fn compile_count_subquery_distinct_target(
    return_clause: &Return,
    path: &str,
    feature_name: &'static str,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    if return_clause.star || return_clause.items.len() != 1 {
        return Err(unsupported(
            format!("{path}.return.items"),
            format!(
                "RETURN DISTINCT inside {feature_name} currently supports exactly one scalar projection"
            ),
        ));
    }
    let item = return_clause.items.first().ok_or_else(|| {
        unsupported(
            format!("{path}.return.items"),
            format!(
                "RETURN DISTINCT inside {feature_name} currently supports exactly one scalar projection"
            ),
        )
    })?;
    if matches!(item.expression, Expression::Variable(_))
        || matches!(item.expression, Expression::CountStar { .. })
        || expression_contains_aggregate(&item.expression)
        || expression_contains_subquery(&item.expression)
    {
        return Err(unsupported(
            format!("{path}.return.items[0]"),
            format!(
                "RETURN DISTINCT inside {feature_name} currently supports exactly one scalar projection"
            ),
        ));
    }
    compile_scalar_expression_in_predicate_mode(
        &item.expression,
        format!("{path}.return.items[0].expression"),
        PredicateCompileMode::Graph {
            plan,
            path_state: Some(state),
        },
        context,
    )
}

fn validate_scoped_subquery_noop_return(
    return_clause: &Return,
    path: &str,
    feature_name: &'static str,
    allow_distinct_noop_return: bool,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    if return_clause.distinct && !allow_distinct_noop_return {
        return Err(unsupported(
            format!("{path}.return.distinct"),
            format!(
                "RETURN DISTINCT inside {feature_name} requires scoped projection planning and is not supported yet"
            ),
        ));
    }
    if return_clause.order.is_some()
        || return_clause.skip.is_some()
        || return_clause.limit.is_some()
    {
        return Err(unsupported(
            format!("{path}.return"),
            format!(
                "RETURN ORDER BY, SKIP, or LIMIT inside {feature_name} requires scoped row-source planning and is not supported yet"
            ),
        ));
    }
    if return_clause.star && return_clause.items.is_empty() {
        return Ok(());
    }
    if return_clause.items.is_empty() {
        return Err(unsupported(
            format!("{path}.return.items"),
            format!("RETURN inside {feature_name} must include RETURN * or scalar projections"),
        ));
    }
    for (index, item) in return_clause.items.iter().enumerate() {
        validate_scoped_subquery_return_item(
            item,
            index,
            path,
            feature_name,
            plan,
            state,
            context,
        )?;
    }
    Ok(())
}

fn validate_scoped_subquery_return_item(
    item: &ProjectionItem,
    index: usize,
    path: &str,
    feature_name: &'static str,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    if scoped_subquery_return_item_is_noop_literal(item) {
        return Ok(());
    }
    if matches!(item.expression, Expression::Variable(_))
        || matches!(item.expression, Expression::CountStar { .. })
        || expression_contains_aggregate(&item.expression)
        || expression_contains_subquery(&item.expression)
    {
        return Err(unsupported(
            format!("{path}.return.items[{index}]"),
            format!(
                "RETURN inside {feature_name} currently supports only row-preserving scalar or literal projections or RETURN *"
            ),
        ));
    }
    compile_scalar_expression_in_predicate_mode(
        &item.expression,
        format!("{path}.return.items[{index}].expression"),
        PredicateCompileMode::Graph {
            plan,
            path_state: Some(state),
        },
        context,
    )?;
    Ok(())
}

fn scoped_collect_subquery_return_clause<'a>(
    body: &'a SinglePartBody,
    path: &str,
) -> Result<&'a Return, CoreError> {
    match body {
        SinglePartBody::Return(return_clause) => Ok(return_clause),
        SinglePartBody::Updating {
            updating,
            return_clause: Some(return_clause),
        } if updating.is_empty() => Ok(return_clause),
        SinglePartBody::Updating {
            updating,
            return_clause: None,
        } if updating.is_empty() => Err(unsupported(
            format!("{path}.return"),
            "COLLECT subqueries require one scalar RETURN projection",
        )),
        SinglePartBody::Finish(_) => Err(unsupported(
            format!("{path}.return"),
            "COLLECT subqueries require one scalar RETURN projection",
        )),
        SinglePartBody::Updating { .. } => Err(unsupported(
            format!("{path}.updating"),
            "write clauses are not supported by Coral virtual graphs",
        )),
    }
}

fn validate_scoped_collect_subquery_return<'a>(
    return_clause: &'a Return,
    path: &str,
    feature_name: &'static str,
) -> Result<&'a ProjectionItem, CoreError> {
    if return_clause.order.is_some()
        || return_clause.skip.is_some()
        || return_clause.limit.is_some()
    {
        return Err(unsupported(
            format!("{path}.return"),
            format!(
                "RETURN ORDER BY, SKIP, or LIMIT inside {feature_name} requires scoped row-source planning and is not supported yet"
            ),
        ));
    }
    if return_clause.star || return_clause.items.len() != 1 {
        return Err(unsupported(
            format!("{path}.return.items"),
            "COLLECT subqueries require exactly one scalar RETURN projection",
        ));
    }
    let item = return_clause.items.first().ok_or_else(|| {
        unsupported(
            format!("{path}.return.items"),
            "COLLECT subqueries require exactly one scalar RETURN projection",
        )
    })?;
    if matches!(item.expression, Expression::CountStar { .. })
        || expression_contains_aggregate(&item.expression)
    {
        return Err(unsupported(
            format!("{path}.return.items[0].expression"),
            "aggregate projections inside COLLECT subqueries require scoped aggregation planning and are not supported yet",
        ));
    }
    Ok(item)
}

fn scoped_subquery_return_item_is_noop_literal(item: &ProjectionItem) -> bool {
    matches!(
        item.expression,
        Expression::Literal(
            CypherLiteral::Number(_)
                | CypherLiteral::String(_)
                | CypherLiteral::Boolean(_)
                | CypherLiteral::Null
        )
    )
}

#[derive(Debug, Clone, Copy)]
struct ScopedPlanDelta {
    nodes_before: usize,
    relationship_base: usize,
    predicate_offset: usize,
}

fn compile_scoped_pattern_predicate(
    part: &PatternPart,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
    feature_name: &'static str,
    missing_context_message: &'static str,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let Some(plan) = plan else {
        return Err(unsupported(path, missing_context_message));
    };
    let mut exists_plan = plan.clone();
    exists_plan.predicate = None;
    let mut exists_state = CypherCompileState::default();
    let node_start = exists_plan.nodes.len();
    let relationship_start = exists_plan.relationships.len();
    let predicate_start = exists_plan.predicates.len();
    compile_pattern_part_into(part, 0, false, &mut exists_plan, &mut exists_state, context)?;
    compile_scoped_plan_delta_predicate(
        exists_plan,
        plan,
        ScopedPlanDelta {
            nodes_before: node_start,
            relationship_base: relationship_start,
            predicate_offset: predicate_start,
        },
        path,
        feature_name,
    )
}

fn compile_scoped_pattern_where_predicate(
    part: &PatternPart,
    where_clause: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
    feature_name: &'static str,
    missing_context_message: &'static str,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let Some(plan) = plan else {
        return Err(unsupported(path, missing_context_message));
    };
    let mut exists_plan = plan.clone();
    exists_plan.predicate = None;
    let mut exists_state = CypherCompileState::default();
    let node_start = exists_plan.nodes.len();
    let relationship_start = exists_plan.relationships.len();
    let predicate_start = exists_plan.predicates.len();
    compile_pattern_part_into(part, 0, false, &mut exists_plan, &mut exists_state, context)?;
    let predicate =
        compile_predicate_expression(where_clause, format!("{path}.where"), &exists_plan, context)?;
    append_predicate_expression(predicate, &mut exists_plan);
    compile_scoped_plan_delta_predicate(
        exists_plan,
        plan,
        ScopedPlanDelta {
            nodes_before: node_start,
            relationship_base: relationship_start,
            predicate_offset: predicate_start,
        },
        path,
        feature_name,
    )
}

fn compile_scoped_plan_delta_predicate(
    exists_plan: GraphPlan,
    plan: &GraphPlan,
    delta: ScopedPlanDelta,
    path: impl Into<String>,
    feature_name: &'static str,
) -> Result<PredicateExpression, CoreError> {
    compile_scoped_plan_delta_pattern(exists_plan, plan, delta, path, feature_name)
        .map(PredicateExpression::ExistsPattern)
}

fn compile_scoped_plan_delta_pattern(
    exists_plan: GraphPlan,
    _plan: &GraphPlan,
    delta: ScopedPlanDelta,
    path: impl Into<String>,
    feature_name: &'static str,
) -> Result<ExistsPatternPredicate, CoreError> {
    compile_scoped_plan_delta_relationship_pattern(exists_plan, delta, path, feature_name)
}

fn compile_scoped_plan_delta_relationship_pattern(
    mut exists_plan: GraphPlan,
    delta: ScopedPlanDelta,
    path: impl Into<String>,
    feature_name: &'static str,
) -> Result<ExistsPatternPredicate, CoreError> {
    let path = path.into();
    let relationships = exists_plan
        .relationships
        .get(delta.relationship_base..)
        .ok_or_else(|| CoreError::internal("EXISTS relationship slice was invalid"))?
        .to_vec();
    let nodes = exists_plan
        .nodes
        .get(delta.nodes_before..)
        .ok_or_else(|| CoreError::internal("EXISTS node slice was invalid"))?
        .to_vec();
    if relationships.is_empty() && nodes.is_empty() {
        return Err(unsupported(
            format!("{path}.pattern"),
            format!("{feature_name} require at least one local node or relationship pattern"),
        ));
    }

    let predicate_parts = take_scoped_plan_delta_predicates(&mut exists_plan, delta)?;

    Ok(ExistsPatternPredicate {
        nodes,
        relationships,
        predicates: predicate_parts.predicates,
        predicate: predicate_parts.predicate.map(Box::new),
    })
}

fn compile_scoped_plan_delta_count_subquery(
    mut count_plan: GraphPlan,
    delta: ScopedPlanDelta,
    path: impl Into<String>,
    feature_name: &'static str,
) -> Result<CountSubqueryPattern, CoreError> {
    let path = path.into();
    let relationships = count_plan
        .relationships
        .get(delta.relationship_base..)
        .ok_or_else(|| CoreError::internal("COUNT relationship slice was invalid"))?
        .to_vec();
    if !relationships.is_empty() {
        return compile_scoped_plan_delta_relationship_pattern(
            count_plan,
            delta,
            path,
            feature_name,
        )
        .map(CountSubqueryPattern::Relationships);
    }

    let nodes = count_plan
        .nodes
        .get(delta.nodes_before..)
        .ok_or_else(|| CoreError::internal("COUNT node slice was invalid"))?
        .to_vec();
    if nodes.is_empty() {
        return Err(unsupported(
            format!("{path}.pattern.nodes"),
            "COUNT subqueries without relationship patterns must bind at least one local node",
        ));
    }
    let predicate_parts = take_scoped_plan_delta_predicates(&mut count_plan, delta)?;
    Ok(CountSubqueryPattern::Nodes {
        nodes,
        predicates: predicate_parts.predicates,
        predicate: predicate_parts.predicate.map(Box::new),
    })
}

#[derive(Debug)]
struct ScopedPredicateParts {
    predicates: Vec<PropertyPredicate>,
    predicate: Option<PredicateExpression>,
}

fn take_scoped_plan_delta_predicates(
    plan: &mut GraphPlan,
    delta: ScopedPlanDelta,
) -> Result<ScopedPredicateParts, CoreError> {
    let predicates = plan
        .predicates
        .get(delta.predicate_offset..)
        .ok_or_else(|| CoreError::internal("scoped predicate slice was invalid"))?
        .to_vec();
    let predicate = plan.predicate.take();
    Ok(ScopedPredicateParts {
        predicates,
        predicate,
    })
}
