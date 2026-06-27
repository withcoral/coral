use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use decypher::ast::clause::{Match, ProjectionItem, Return, SortDirection, With};
use decypher::ast::expr::{
    BinaryOperator as CypherBinaryOperator, CaseExpression,
    ComparisonOperator as CypherComparisonOperator, Expression, FunctionInvocation,
    Literal as CypherLiteral, NumberLiteral, Parameter as CypherParameter, UnaryOperator,
};
use decypher::ast::names::{SymbolicName, Variable};
use decypher::ast::pattern::{
    LabelExpression, NodePattern as CypherNodePattern, PatternElement, PatternPart, Properties,
    Quantifier, RangeLiteral, RelationshipDirection as CypherRelationshipDirection,
    RelationshipPattern as CypherRelationshipPattern,
};
use decypher::ast::query::{
    MultiPartQuery, MultiPartQueryPart, Query, QueryBody, ReadingClause, RegularQuery,
    SinglePartBody, SinglePartQuery, SingleQuery, SingleQueryKind,
};
use decypher::cst::{AstNode as _, AstToken as _, Ident};
use decypher::syntax::{SyntaxKind, SyntaxNode};
use ordered_float::OrderedFloat;
use regex::Regex;

use super::diagnostic::Diagnostic;
use super::ir::{
    AggregateFunction, AggregateTarget, ArithmeticOperator, ComparisonOperator, Direction,
    ElementIdPredicate, GraphPlan, GraphQuery, GraphUnion, GraphUnionBranch,
    GraphUnionOuterProjection, GraphUnionOuterProjectionItem, KeyPredicate, Literal, NodePattern,
    OptionalMatchScope, OrderDirection, OrderExpression, OrderKey, PredicateExpression,
    PredicateRhs, PresencePredicate, Projection, ProjectionPredicate,
    ProjectionPredicateExpression, ProjectionPredicateRhs, PropertyKeyMembershipPredicate,
    PropertyPredicate, PropertyRef, RelationshipPattern, ScalarCaseAlternative, ScalarExpression,
    ScalarPredicate, ScalarPredicateRhs,
};
use crate::CoreError;

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

const MAX_PATTERN_ALTERNATIVE_BRANCHES: usize = 64;
const MAX_FIXED_RELATIONSHIP_LENGTH: usize = 8;
const INTERNAL_GRAPH_IDENTITY_FUNCTION: &str = "__coral_graph_identity";
const INTERNAL_GRAPH_PRESENCE_FUNCTION: &str = "__coral_graph_presence";

#[derive(Debug, Clone)]
enum StaticLabelTypeAlternativeSite {
    SinglePart {
        reading_clause_index: usize,
        pattern_part_index: usize,
        target: PatternAlternativeTarget,
        alternatives: Vec<LabelTypeAlternative>,
    },
    MultiPart {
        query_part: MultiPartAlternativePart,
        reading_clause_index: usize,
        pattern_part_index: usize,
        target: PatternAlternativeTarget,
        alternatives: Vec<LabelTypeAlternative>,
    },
}

#[derive(Debug, Clone, Copy)]
enum MultiPartAlternativePart {
    Part(usize),
    FinalPart,
}

#[derive(Debug, Clone, Copy)]
enum PatternAlternativeTarget {
    StartNode,
    ChainNode(usize),
    Relationship(usize),
}

#[derive(Debug, Clone)]
enum LabelTypeAlternative {
    NodeLabels(Vec<LabelExpression>),
    RelationshipType(LabelExpression),
}

#[derive(Debug, Clone)]
enum BoundedRelationshipRangeSite {
    SinglePart {
        reading_clause_index: usize,
        pattern_part_index: usize,
        chain_index: usize,
        target: RelationshipRangeTarget,
        alternatives: Vec<usize>,
    },
    MultiPart {
        query_part: MultiPartAlternativePart,
        reading_clause_index: usize,
        pattern_part_index: usize,
        chain_index: usize,
        target: RelationshipRangeTarget,
        alternatives: Vec<usize>,
    },
}

#[derive(Debug, Clone, Copy)]
enum RelationshipRangeTarget {
    DetailRange,
    Quantifier,
}

type BoundedRelationshipRangeSiteInfo = (usize, usize, usize, RelationshipRangeTarget, Vec<usize>);
type MatchBoundedRelationshipRangeSiteInfo = (usize, usize, RelationshipRangeTarget, Vec<usize>);

#[derive(Debug, Clone, Copy)]
enum RelationshipEndpoint {
    Start,
    End,
}

#[derive(Debug, Clone, Copy)]
struct PathBinding {
    length: usize,
}

#[derive(Debug, Default)]
struct CypherCompileState {
    path_variables: BTreeMap<String, PathBinding>,
    hidden_graph_variables: BTreeSet<String>,
    out_of_scope_graph_names: BTreeSet<String>,
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
    match compile_cypher_query_with_parameters(cypher, parameters)? {
        GraphQuery::Plan(plan) => Ok(plan),
        GraphQuery::Union(_) => Err(unsupported(
            "query.union",
            "compile_cypher returns a single graph plan; use compile_cypher_query for UNION queries",
        )),
    }
}

/// Parses and compiles Cypher into a read-only virtual graph query.
///
/// This accepts the same single-query subset as [`compile_cypher`] plus
/// top-level `UNION` / `UNION ALL` composition between supported branch queries.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed or uses
/// Cypher/GQL features outside Coral's current read-only virtual graph subset.
pub fn compile_cypher_query(cypher: &str) -> Result<GraphQuery, CoreError> {
    compile_cypher_query_with_parameters(cypher, &BTreeMap::new())
}

/// Parses and compiles Cypher with typed parameter values into a read-only graph query.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed, uses
/// unsupported Cypher/GQL features, references a missing parameter, or binds a
/// parameter value in an unsupported position.
pub fn compile_cypher_query_with_parameters(
    cypher: &str,
    parameters: &BTreeMap<String, CypherParameterValue>,
) -> Result<GraphQuery, CoreError> {
    let query = decypher::parse(cypher).map_err(|error| {
        Diagnostic::new("CYPHER_PARSE_ERROR", "query", error.to_string()).into_core_error()
    })?;
    let context = CypherCompileContext::from_source_with_parameters(cypher, parameters.clone());
    compile_query(&query, &context)
}

fn compile_query(query: &Query, context: &CypherCompileContext) -> Result<GraphQuery, CoreError> {
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

    match statement {
        QueryBody::SingleQuery(single_query) => {
            compile_single_query_as_graph_query(single_query, context, "query")
        }
        QueryBody::Regular(regular_query) => compile_regular_query(regular_query, context),
        _ => Err(unsupported(
            "query",
            "only read-only MATCH queries and UNION queries are supported",
        )),
    }
}

fn compile_single_query_as_graph_query(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<GraphQuery, CoreError> {
    let path = path.into();
    let mut variants = expand_single_query_pattern_alternatives(single_query)?;
    if variants.len() == 1 {
        let plan = compile_single_query(
            variants.first().ok_or_else(|| {
                CoreError::internal("Cypher query expansion produced no variants")
            })?,
            context,
        )?;
        return Ok(GraphQuery::Plan(plan));
    }

    validate_static_label_type_alternative_expansion_supported(single_query, &path, context)?;
    let outer_projection_plan =
        analyze_static_alternative_outer_projection(single_query, &path, context)?;
    let hidden_order_plan = analyze_static_alternative_hidden_order(
        single_query,
        outer_projection_plan.as_ref(),
        &path,
    )?;
    for variant in &mut variants {
        apply_static_alternative_outer_projection_rewrite(
            variant,
            outer_projection_plan.as_ref(),
            &path,
        )?;
        apply_static_alternative_hidden_order_rewrite(variant, hidden_order_plan.as_ref(), &path)?;
        clear_final_return_outer_modifiers(variant, &path)?;
    }
    let plans = variants
        .iter()
        .map(|variant| compile_single_query(variant, context))
        .collect::<Result<Vec<_>, _>>()?;
    let projection_names = plans
        .first()
        .map(GraphPlan::projection_output_names)
        .ok_or_else(|| CoreError::internal("Cypher query expansion produced no graph plans"))?;
    let outer_projection = compile_static_alternative_outer_projection(
        outer_projection_plan.as_ref(),
        &projection_names,
    )?;
    let outer_projection = compile_static_alternative_hidden_order_outer_projection(
        outer_projection,
        hidden_order_plan.as_ref(),
        &projection_names,
        final_return_clause(single_query, &path)?.items.len(),
    )?;
    let projection_names = outer_projection.as_ref().map_or_else(
        || projection_names.clone(),
        GraphUnionOuterProjection::output_names,
    );
    let order_by = compile_static_alternative_outer_order_by(
        single_query,
        &projection_names,
        hidden_order_plan.as_ref(),
        &path,
    )?;
    let (skip, limit) = compile_static_alternative_outer_skip_limit(single_query, context, &path)?;
    let distinct = final_return_clause(single_query, &path)?.distinct;
    graph_query_from_alternative_plans(plans, outer_projection, distinct, order_by, skip, limit)
}

fn graph_query_from_alternative_plans(
    mut plans: Vec<GraphPlan>,
    outer_projection: Option<GraphUnionOuterProjection>,
    distinct: bool,
    order_by: Vec<OrderKey>,
    skip: Option<u64>,
    limit: Option<u64>,
) -> Result<GraphQuery, CoreError> {
    if plans.is_empty() {
        return Err(CoreError::internal(
            "Cypher query expansion produced no graph plans",
        ));
    }
    let first = plans.remove(0);
    if plans.is_empty() {
        return Ok(GraphQuery::Plan(first));
    }
    Ok(GraphQuery::Union(GraphUnion {
        first,
        branches: plans
            .into_iter()
            .map(|plan| GraphUnionBranch { all: true, plan })
            .collect(),
        outer_projection,
        distinct,
        order_by,
        skip,
        limit,
    }))
}

fn clear_final_return_outer_modifiers(
    single_query: &mut SingleQuery,
    path: &str,
) -> Result<(), CoreError> {
    let return_clause = final_return_clause_mut(single_query, path)?;
    return_clause.distinct = false;
    return_clause.order = None;
    return_clause.skip = None;
    return_clause.limit = None;
    Ok(())
}

#[derive(Debug, Clone)]
struct StaticAlternativeOuterProjectionPlan {
    items: Vec<StaticAlternativeOuterProjectionItem>,
    group_item_indices: Vec<usize>,
}

#[derive(Debug, Clone)]
enum StaticAlternativeOuterProjectionItem {
    Column {
        return_index: usize,
    },
    CountAll {
        alias: String,
    },
    Aggregate {
        function: AggregateFunction,
        source_alias: String,
        source_expression: Box<Expression>,
        distinct: bool,
        alias: String,
    },
}

fn analyze_static_alternative_outer_projection(
    single_query: &SingleQuery,
    path: &str,
    context: &CypherCompileContext,
) -> Result<Option<StaticAlternativeOuterProjectionPlan>, CoreError> {
    let return_clause = final_return_clause(single_query, path)?;
    let mut items = Vec::new();
    let mut group_item_indices = Vec::new();
    let has_outer_aggregate = return_clause
        .items
        .iter()
        .any(|item| expression_contains_aggregate(&item.expression));

    for (index, item) in return_clause.items.iter().enumerate() {
        if let Some(alias) = count_star_item_alias(item) {
            items.push(StaticAlternativeOuterProjectionItem::CountAll { alias });
        } else if let Some(item) =
            compile_static_alternative_outer_aggregate_item(item, index, path, context)?
        {
            items.push(item);
        } else if expression_contains_aggregate(&item.expression) {
            return Err(unsupported(
                format!("{path}.return.items[{index}].expression"),
                "static label/type alternatives with property or non-count aggregate RETURN projections require staged query planning and are not supported yet",
            ));
        } else if has_outer_aggregate {
            group_item_indices.push(index);
            items.push(StaticAlternativeOuterProjectionItem::Column {
                return_index: index,
            });
        }
    }

    if has_outer_aggregate {
        return Ok(Some(StaticAlternativeOuterProjectionPlan {
            items,
            group_item_indices,
        }));
    }
    Ok(None)
}

fn compile_static_alternative_outer_projection(
    plan: Option<&StaticAlternativeOuterProjectionPlan>,
    branch_projection_names: &[String],
) -> Result<Option<GraphUnionOuterProjection>, CoreError> {
    let Some(plan) = plan else {
        return Ok(None);
    };
    let mut group_names = BTreeMap::new();
    for (position, return_index) in plan.group_item_indices.iter().enumerate() {
        let name = branch_projection_names
            .get(position)
            .cloned()
            .ok_or_else(|| {
                CoreError::internal("static alternative group projection names were not aligned")
            })?;
        group_names.insert(*return_index, name);
    }

    let mut items = Vec::with_capacity(plan.items.len());
    for item in &plan.items {
        match item {
            StaticAlternativeOuterProjectionItem::Column { return_index } => {
                let name = group_names.get(return_index).cloned().ok_or_else(|| {
                    CoreError::internal(
                        "static alternative group projection item had no branch output name",
                    )
                })?;
                items.push(GraphUnionOuterProjectionItem::Column { name });
            }
            StaticAlternativeOuterProjectionItem::CountAll { alias } => {
                items.push(GraphUnionOuterProjectionItem::CountAll {
                    alias: alias.clone(),
                });
            }
            StaticAlternativeOuterProjectionItem::Aggregate {
                function,
                source_alias,
                distinct,
                alias,
                ..
            } => items.push(GraphUnionOuterProjectionItem::Aggregate {
                function: *function,
                source: source_alias.clone(),
                distinct: *distinct,
                alias: alias.clone(),
            }),
        }
    }

    Ok(Some(GraphUnionOuterProjection {
        items,
        group_by: plan
            .group_item_indices
            .iter()
            .map(|return_index| {
                group_names.get(return_index).cloned().ok_or_else(|| {
                    CoreError::internal(
                        "static alternative group projection item had no GROUP BY name",
                    )
                })
            })
            .collect::<Result<Vec<_>, CoreError>>()?,
    }))
}

fn apply_static_alternative_outer_projection_rewrite(
    single_query: &mut SingleQuery,
    outer_projection: Option<&StaticAlternativeOuterProjectionPlan>,
    path: &str,
) -> Result<(), CoreError> {
    let Some(outer_projection) = outer_projection else {
        return Ok(());
    };
    let return_clause = final_return_clause_mut(single_query, path)?;
    return_clause.distinct = false;
    let aggregate_source_items = outer_projection
        .items
        .iter()
        .filter_map(|item| match item {
            StaticAlternativeOuterProjectionItem::Aggregate {
                source_alias,
                source_expression,
                ..
            } => Some((source_alias, source_expression.as_ref())),
            StaticAlternativeOuterProjectionItem::Column { .. }
            | StaticAlternativeOuterProjectionItem::CountAll { .. } => None,
        })
        .collect::<Vec<_>>();
    if outer_projection.group_item_indices.is_empty() && aggregate_source_items.is_empty() {
        let span = return_clause.span;
        return_clause.items = vec![ProjectionItem {
            expression: Expression::Literal(CypherLiteral::Number(NumberLiteral::Integer(1))),
            alias: Some(Variable {
                name: SymbolicName {
                    name: "__coral_count_row".to_string(),
                    span,
                },
            }),
        }];
    } else {
        let original_items = return_clause.items.clone();
        let mut rewritten_items = outer_projection
            .group_item_indices
            .iter()
            .map(|index| {
                original_items.get(*index).cloned().ok_or_else(|| {
                    CoreError::internal(
                        "static alternative group projection index was out of bounds",
                    )
                })
            })
            .collect::<Result<Vec<_>, CoreError>>()?;
        let span = return_clause.span;
        rewritten_items.extend(aggregate_source_items.into_iter().map(
            |(source_alias, source_expression)| ProjectionItem {
                expression: source_expression.clone(),
                alias: Some(Variable {
                    name: SymbolicName {
                        name: source_alias.clone(),
                        span,
                    },
                }),
            },
        ));
        return_clause.items = rewritten_items;
    }
    Ok(())
}

fn compile_static_alternative_outer_aggregate_item(
    item: &ProjectionItem,
    index: usize,
    path: &str,
    context: &CypherCompileContext,
) -> Result<Option<StaticAlternativeOuterProjectionItem>, CoreError> {
    let Some(function) = aggregate_function_call(&item.expression) else {
        return Ok(None);
    };
    let function_kind = compile_aggregate_function(function).ok_or_else(|| {
        unsupported(
            format!("{path}.return.items[{index}].expression"),
            "static label/type alternatives only support property aggregates after expansion",
        )
    })?;
    reject_unsupported_distinct_aggregate(
        function_kind,
        function.distinct,
        format!("{path}.return.items[{index}].expression.distinct"),
    )?;
    let target = compile_function_aggregate_target(
        function,
        function_kind,
        &format!("{path}.return.items[{index}]"),
        None,
        context,
    )?;
    let source_expression = match target {
        AggregateTarget::Property(_) => {
            let [argument] = function.arguments.as_slice() else {
                return Err(unsupported(
                    format!("{path}.return.items[{index}].expression.arguments"),
                    "static label/type alternatives with property aggregates require one graph property argument",
                ));
            };
            argument.clone()
        }
        AggregateTarget::VariableKey { variable } => {
            if function_kind != AggregateFunction::Count {
                return Err(unsupported(
                    format!("{path}.return.items[{index}].expression.arguments"),
                    "static label/type alternatives only support count(variable) over graph variables",
                ));
            }
            if function.distinct {
                graph_identity_function_expression_for_variable(&variable, function)
            } else {
                graph_presence_function_expression_for_variable(&variable, function)
            }
        }
    };
    Ok(Some(StaticAlternativeOuterProjectionItem::Aggregate {
        function: function_kind,
        source_alias: format!("__coral_agg_{index}"),
        source_expression: Box::new(source_expression),
        distinct: function.distinct,
        alias: item.alias.as_ref().map_or_else(
            || aggregate_function_name(function_kind).to_string(),
            variable_name,
        ),
    }))
}

fn graph_identity_function_expression_for_variable(
    variable: &str,
    source_function: &FunctionInvocation,
) -> Expression {
    function_expression_for_variable(INTERNAL_GRAPH_IDENTITY_FUNCTION, variable, source_function)
}

fn graph_presence_function_expression_for_variable(
    variable: &str,
    source_function: &FunctionInvocation,
) -> Expression {
    function_expression_for_variable(INTERNAL_GRAPH_PRESENCE_FUNCTION, variable, source_function)
}

fn function_expression_for_variable(
    function_name: &str,
    variable: &str,
    source_function: &FunctionInvocation,
) -> Expression {
    let span = source_function.span;
    Expression::FunctionCall(FunctionInvocation {
        name: vec![SymbolicName {
            name: function_name.to_string(),
            span,
        }],
        distinct: false,
        arguments: vec![Expression::Variable(Variable {
            name: SymbolicName {
                name: variable.to_string(),
                span,
            },
        })],
        span,
    })
}

fn aggregate_function_call(expression: &Expression) -> Option<&FunctionInvocation> {
    match expression {
        Expression::Parenthesized(inner) => aggregate_function_call(inner),
        Expression::FunctionCall(function) if compile_aggregate_function(function).is_some() => {
            Some(function)
        }
        _ => None,
    }
}

fn count_star_item_alias(item: &ProjectionItem) -> Option<String> {
    if !matches!(item.expression, Expression::CountStar { .. }) {
        return None;
    }
    Some(
        item.alias
            .as_ref()
            .map_or_else(|| "count".to_string(), variable_name),
    )
}

#[derive(Debug, Clone)]
struct StaticAlternativeHiddenOrderPlan {
    items: Vec<StaticAlternativeHiddenOrderItem>,
}

#[derive(Debug, Clone)]
struct StaticAlternativeHiddenOrderItem {
    order_index: usize,
    expression: Expression,
    alias: String,
}

impl StaticAlternativeHiddenOrderPlan {
    fn alias_for_order_index(&self, order_index: usize) -> Option<&str> {
        self.items
            .iter()
            .find(|item| item.order_index == order_index)
            .map(|item| item.alias.as_str())
    }
}

fn analyze_static_alternative_hidden_order(
    single_query: &SingleQuery,
    outer_projection: Option<&StaticAlternativeOuterProjectionPlan>,
    path: &str,
) -> Result<Option<StaticAlternativeHiddenOrderPlan>, CoreError> {
    let return_clause = final_return_clause(single_query, path)?;
    let Some(order) = &return_clause.order else {
        return Ok(None);
    };
    let mut hidden_items = Vec::new();
    let projection_names = return_clause
        .items
        .iter()
        .map(return_item_projection_name)
        .collect::<Vec<_>>();
    for (index, item) in order.items.iter().enumerate() {
        if resolve_projected_static_alternative_outer_order_alias(
            &item.expression,
            return_clause,
            &projection_names,
            format!("{path}.return.order.items[{index}].expression"),
        )?
        .is_some()
        {
            continue;
        }
        if outer_projection.is_some() {
            return Err(unsupported(
                format!("{path}.return.order.items[{index}].expression"),
                "static label/type alternatives with aggregate RETURN projections cannot ORDER BY unprojected expressions yet",
            ));
        }
        if return_clause.distinct {
            return Err(unsupported(
                format!("{path}.return.order.items[{index}].expression"),
                "static label/type alternatives with RETURN DISTINCT cannot ORDER BY unprojected expressions yet",
            ));
        }
        hidden_items.push(StaticAlternativeHiddenOrderItem {
            order_index: index,
            expression: item.expression.clone(),
            alias: format!("__coral_order_{index}"),
        });
    }
    if hidden_items.is_empty() {
        Ok(None)
    } else {
        Ok(Some(StaticAlternativeHiddenOrderPlan {
            items: hidden_items,
        }))
    }
}

fn return_item_projection_name(item: &ProjectionItem) -> String {
    item.alias.as_ref().map_or_else(
        || match &item.expression {
            Expression::PropertyLookup { base, property, .. } => match base.as_ref() {
                Expression::Variable(variable) => {
                    format!("{}_{}", variable_name(variable), property.name.name)
                }
                _ => "expression".to_string(),
            },
            Expression::Variable(variable) => variable_name(variable),
            Expression::CountStar { .. } => "count".to_string(),
            Expression::FunctionCall(function) => {
                if let Some(function_kind) = compile_aggregate_function(function) {
                    aggregate_function_name(function_kind).to_string()
                } else {
                    default_scalar_function_alias(function)
                }
            }
            Expression::Case(_) => "case".to_string(),
            _ => "expression".to_string(),
        },
        variable_name,
    )
}

fn apply_static_alternative_hidden_order_rewrite(
    single_query: &mut SingleQuery,
    hidden_order: Option<&StaticAlternativeHiddenOrderPlan>,
    path: &str,
) -> Result<(), CoreError> {
    let Some(hidden_order) = hidden_order else {
        return Ok(());
    };
    let return_clause = final_return_clause_mut(single_query, path)?;
    let span = return_clause.span;
    return_clause
        .items
        .extend(hidden_order.items.iter().map(|item| ProjectionItem {
            expression: item.expression.clone(),
            alias: Some(Variable {
                name: SymbolicName {
                    name: item.alias.clone(),
                    span,
                },
            }),
        }));
    Ok(())
}

fn compile_static_alternative_hidden_order_outer_projection(
    outer_projection: Option<GraphUnionOuterProjection>,
    hidden_order: Option<&StaticAlternativeHiddenOrderPlan>,
    branch_projection_names: &[String],
    return_item_count: usize,
) -> Result<Option<GraphUnionOuterProjection>, CoreError> {
    if hidden_order.is_none() {
        return Ok(outer_projection);
    }
    if outer_projection.is_some() {
        return Err(CoreError::internal(
            "hidden static alternative ORDER BY should have been rejected for aggregate outer projections",
        ));
    }
    let items = branch_projection_names
        .get(..return_item_count)
        .ok_or_else(|| {
            CoreError::internal(
                "static alternative hidden ORDER BY projection names were not aligned",
            )
        })?
        .iter()
        .cloned()
        .map(|name| GraphUnionOuterProjectionItem::Column { name })
        .collect();
    Ok(Some(GraphUnionOuterProjection {
        items,
        group_by: Vec::new(),
    }))
}

fn compile_static_alternative_outer_order_by(
    single_query: &SingleQuery,
    projection_names: &[String],
    hidden_order: Option<&StaticAlternativeHiddenOrderPlan>,
    path: &str,
) -> Result<Vec<OrderKey>, CoreError> {
    let return_clause = final_return_clause(single_query, path)?;
    let Some(order) = &return_clause.order else {
        return Ok(Vec::new());
    };

    let mut order_by = Vec::with_capacity(order.items.len());
    for (index, item) in order.items.iter().enumerate() {
        let alias = resolve_projected_static_alternative_outer_order_alias(
            &item.expression,
            return_clause,
            projection_names,
            format!("{path}.return.order.items[{index}].expression"),
        )?
        .or_else(|| {
            hidden_order.and_then(|hidden_order| {
                hidden_order
                    .alias_for_order_index(index)
                    .map(ToString::to_string)
            })
        })
        .ok_or_else(|| {
            unsupported(
                format!("{path}.return.order.items[{index}].expression"),
                "static label/type alternatives with global ORDER BY currently require projected aliases, projected expressions, or row-preserving hidden sort expressions",
            )
        })?;
        order_by.push(OrderKey {
            expression: OrderExpression::ProjectionAlias(alias),
            direction: match item.direction {
                Some(SortDirection::Descending) => OrderDirection::Descending,
                Some(SortDirection::Ascending) | None => OrderDirection::Ascending,
            },
            nulls: None,
        });
    }
    Ok(order_by)
}

fn resolve_projected_static_alternative_outer_order_alias(
    expression: &Expression,
    return_clause: &Return,
    projection_names: &[String],
    path: impl Into<String>,
) -> Result<Option<String>, CoreError> {
    let path = path.into();
    if let Expression::Variable(variable) = expression {
        let alias = variable_name(variable);
        if projection_names.iter().any(|name| name == &alias) {
            return Ok(Some(alias));
        }
        return Err(unsupported(
            path,
            format!("ORDER BY alias '{alias}' does not match a RETURN projection"),
        ));
    }

    for (index, item) in return_clause.items.iter().enumerate() {
        if expressions_equivalent_ignoring_span(&item.expression, expression) {
            return projection_names
                .get(index)
                .cloned()
                .map(Some)
                .ok_or_else(|| {
                    CoreError::internal(
                        "RETURN projection names were not aligned with RETURN items",
                    )
                });
        }
    }

    Ok(None)
}

fn expressions_equivalent_ignoring_span(left: &Expression, right: &Expression) -> bool {
    match (left, right) {
        (Expression::Parenthesized(left), right) => {
            expressions_equivalent_ignoring_span(left, right)
        }
        (left, Expression::Parenthesized(right)) => {
            expressions_equivalent_ignoring_span(left, right)
        }
        (Expression::Variable(left), Expression::Variable(right)) => {
            variable_name(left) == variable_name(right)
        }
        (Expression::Parameter(left), Expression::Parameter(right)) => {
            left.name.name == right.name.name
        }
        (
            Expression::PropertyLookup {
                base: left_base,
                property: left_property,
                ..
            },
            Expression::PropertyLookup {
                base: right_base,
                property: right_property,
                ..
            },
        ) => {
            left_property.name.name == right_property.name.name
                && expressions_equivalent_ignoring_span(left_base, right_base)
        }
        (Expression::FunctionCall(left), Expression::FunctionCall(right)) => {
            left.name.len() == right.name.len()
                && left
                    .name
                    .iter()
                    .zip(&right.name)
                    .all(|(left, right)| left.name == right.name)
                && left.distinct == right.distinct
                && left.arguments.len() == right.arguments.len()
                && left
                    .arguments
                    .iter()
                    .zip(&right.arguments)
                    .all(|(left, right)| expressions_equivalent_ignoring_span(left, right))
        }
        (
            Expression::UnaryOp {
                op: left_op,
                operand: left_operand,
                ..
            },
            Expression::UnaryOp {
                op: right_op,
                operand: right_operand,
                ..
            },
        ) => {
            left_op == right_op && expressions_equivalent_ignoring_span(left_operand, right_operand)
        }
        (
            Expression::BinaryOp {
                op: left_op,
                lhs: left_lhs,
                rhs: left_rhs,
                ..
            },
            Expression::BinaryOp {
                op: right_op,
                lhs: right_lhs,
                rhs: right_rhs,
                ..
            },
        ) => {
            left_op == right_op
                && expressions_equivalent_ignoring_span(left_lhs, right_lhs)
                && expressions_equivalent_ignoring_span(left_rhs, right_rhs)
        }
        (Expression::Literal(left), Expression::Literal(right)) => {
            literals_equivalent_ignoring_span(left, right)
        }
        (Expression::CountStar { .. }, Expression::CountStar { .. }) => true,
        _ => false,
    }
}

fn literals_equivalent_ignoring_span(left: &CypherLiteral, right: &CypherLiteral) -> bool {
    match (left, right) {
        (CypherLiteral::Number(left), CypherLiteral::Number(right)) => left == right,
        (CypherLiteral::String(left), CypherLiteral::String(right)) => left.value == right.value,
        (CypherLiteral::Boolean(left), CypherLiteral::Boolean(right)) => left == right,
        (CypherLiteral::Null, CypherLiteral::Null) => true,
        (CypherLiteral::List(left), CypherLiteral::List(right)) => {
            left.elements.len() == right.elements.len()
                && left
                    .elements
                    .iter()
                    .zip(&right.elements)
                    .all(|(left, right)| expressions_equivalent_ignoring_span(left, right))
        }
        (CypherLiteral::Map(left), CypherLiteral::Map(right)) => {
            left.entries.len() == right.entries.len()
                && left.entries.iter().zip(&right.entries).all(
                    |((left_key, left_value), (right_key, right_value))| {
                        left_key.name.name == right_key.name.name
                            && expressions_equivalent_ignoring_span(left_value, right_value)
                    },
                )
        }
        _ => false,
    }
}

fn compile_static_alternative_outer_skip_limit(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
    path: &str,
) -> Result<(Option<u64>, Option<u64>), CoreError> {
    let return_clause = final_return_clause(single_query, path)?;
    let skip = return_clause
        .skip
        .as_ref()
        .map(|skip| compile_skip(skip, format!("{path}.return.skip"), context))
        .transpose()?;
    let limit = return_clause
        .limit
        .as_ref()
        .map(|limit| compile_limit(limit, format!("{path}.return.limit"), context))
        .transpose()?;
    Ok((skip, limit))
}

fn final_return_clause<'a>(
    single_query: &'a SingleQuery,
    path: &str,
) -> Result<&'a Return, CoreError> {
    match &single_query.kind {
        SingleQueryKind::SinglePart(single_part) => {
            return_clause_from_single_part(single_part, path)
        }
        SingleQueryKind::MultiPart(multi_part) => {
            return_clause_from_single_part(&multi_part.final_part, format!("{path}.final_part"))
        }
    }
}

fn final_return_clause_mut<'a>(
    single_query: &'a mut SingleQuery,
    path: &str,
) -> Result<&'a mut Return, CoreError> {
    match &mut single_query.kind {
        SingleQueryKind::SinglePart(single_part) => {
            return_clause_mut_from_single_part(single_part, path)
        }
        SingleQueryKind::MultiPart(multi_part) => return_clause_mut_from_single_part(
            &mut multi_part.final_part,
            format!("{path}.final_part"),
        ),
    }
}

fn return_clause_mut_from_single_part(
    query: &mut SinglePartQuery,
    path: impl Into<String>,
) -> Result<&mut Return, CoreError> {
    let path = path.into();
    match &mut query.body {
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

fn compile_single_query(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
) -> Result<GraphPlan, CoreError> {
    match &single_query.kind {
        SingleQueryKind::SinglePart(single_part) => compile_single_part(single_part, context),
        SingleQueryKind::MultiPart(multi_part) => compile_multi_part(multi_part, context),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExplicitUnionMode {
    All,
    Distinct,
    Mixed,
}

fn explicit_union_mode(query: &RegularQuery) -> ExplicitUnionMode {
    if query.unions.iter().all(|union| union.all) {
        return ExplicitUnionMode::All;
    }
    if query.unions.iter().all(|union| !union.all) {
        return ExplicitUnionMode::Distinct;
    }
    ExplicitUnionMode::Mixed
}

fn compile_regular_query(
    query: &RegularQuery,
    context: &CypherCompileContext,
) -> Result<GraphQuery, CoreError> {
    let first_query =
        compile_single_query_as_graph_query(&query.single_query, context, "query.single_query")?;
    if query.unions.is_empty() {
        return Ok(first_query);
    }

    let union_mode = explicit_union_mode(query);
    let mut flattened = Vec::with_capacity(query.unions.len() + 1);
    let mut flattened_static_alternative_union = false;
    append_explicit_union_component(
        first_query,
        None,
        union_mode,
        "query.single_query",
        &mut flattened,
        &mut flattened_static_alternative_union,
    )?;

    for (index, union) in query.unions.iter().enumerate() {
        let component = compile_single_query_as_graph_query(
            &union.single_query,
            context,
            format!("query.unions[{index}].single_query"),
        )?;
        append_explicit_union_component(
            component,
            Some(union.all),
            union_mode,
            format!("query.unions[{index}].single_query"),
            &mut flattened,
            &mut flattened_static_alternative_union,
        )?;
    }

    let use_outer_distinct =
        union_mode == ExplicitUnionMode::Distinct && flattened_static_alternative_union;
    let mut flattened = flattened.into_iter();
    let (first_all, first) = flattened
        .next()
        .ok_or_else(|| CoreError::internal("explicit union produced no graph plans"))?;
    if first_all.is_some() {
        return Err(CoreError::internal(
            "explicit union first graph plan unexpectedly had a union operator",
        ));
    }
    let expected_projection_names = projection_names(&first);
    let mut branches = Vec::new();
    for (index, (all, plan)) in flattened.enumerate() {
        let leading_all = all.ok_or_else(|| {
            CoreError::internal("explicit union branch graph plan had no union operator")
        })?;
        let projection_names = projection_names(&plan);
        if projection_names != expected_projection_names {
            return Err(unsupported(
                format!("query.union_branches[{index}].return"),
                format!(
                    "UNION branch projections must match the first branch; expected [{}], got [{}]",
                    expected_projection_names.join(", "),
                    projection_names.join(", ")
                ),
            ));
        }
        branches.push(GraphUnionBranch {
            all: if use_outer_distinct {
                true
            } else {
                leading_all
            },
            plan,
        });
    }

    Ok(GraphQuery::Union(GraphUnion {
        first,
        branches,
        outer_projection: None,
        distinct: use_outer_distinct,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    }))
}

fn projection_names(plan: &GraphPlan) -> Vec<String> {
    plan.projection_output_names()
}

fn append_explicit_union_component(
    component: GraphQuery,
    leading_all: Option<bool>,
    union_mode: ExplicitUnionMode,
    path: impl Into<String>,
    output: &mut Vec<(Option<bool>, GraphPlan)>,
    flattened_static_alternative_union: &mut bool,
) -> Result<(), CoreError> {
    let path = path.into();
    match component {
        GraphQuery::Plan(plan) => {
            output.push((leading_all, plan));
            Ok(())
        }
        GraphQuery::Union(union) => {
            if union_mode == ExplicitUnionMode::Mixed {
                return Err(unsupported(
                    path,
                    "static pattern label/type alternatives can be combined with uniform top-level UNION ALL or UNION; mixed UNION and UNION ALL requires nested union grouping",
                ));
            }
            if union.outer_projection.is_some()
                || !union.order_by.is_empty()
                || union.skip.is_some()
                || union.limit.is_some()
            {
                return Err(unsupported(
                    path,
                    "static pattern label/type alternatives with branch-level ORDER BY, SKIP, LIMIT, or aggregate outer projections require nested union grouping",
                ));
            }
            if union.distinct && union_mode != ExplicitUnionMode::Distinct {
                return Err(unsupported(
                    path,
                    "static pattern label/type alternatives with branch-level DISTINCT can only be flattened into uniform top-level UNION distinct",
                ));
            }
            *flattened_static_alternative_union = true;
            output.push((leading_all, union.first));
            for branch in union.branches {
                if !branch.all {
                    return Err(CoreError::internal(
                        "static label/type alternative expansion produced a non-UNION ALL branch",
                    ));
                }
                output.push((Some(true), branch.plan));
            }
            Ok(())
        }
    }
}

fn expand_single_query_pattern_alternatives(
    single_query: &SingleQuery,
) -> Result<Vec<SingleQuery>, CoreError> {
    let mut expanded = vec![single_query.clone()];
    loop {
        let mut progressed = false;
        let mut next = Vec::with_capacity(expanded.len());
        for query in expanded {
            if let Some(site) = first_static_label_type_alternative_site(&query) {
                progressed = true;
                let alternatives = match &site {
                    StaticLabelTypeAlternativeSite::SinglePart { alternatives, .. }
                    | StaticLabelTypeAlternativeSite::MultiPart { alternatives, .. } => {
                        alternatives.clone()
                    }
                };
                for alternative in alternatives {
                    if next.len() >= MAX_PATTERN_ALTERNATIVE_BRANCHES {
                        return Err(unsupported(
                            "query.pattern",
                            format!(
                                "pattern alternatives expand to more than {MAX_PATTERN_ALTERNATIVE_BRANCHES} branches; simplify the pattern or split the query explicitly"
                            ),
                        ));
                    }
                    let mut variant = query.clone();
                    apply_static_label_type_alternative(&mut variant, &site, alternative)?;
                    next.push(variant);
                }
                continue;
            }

            if let Some(site) = first_bounded_relationship_range_site(&query)? {
                progressed = true;
                let alternatives = match &site {
                    BoundedRelationshipRangeSite::SinglePart { alternatives, .. }
                    | BoundedRelationshipRangeSite::MultiPart { alternatives, .. } => {
                        alternatives.clone()
                    }
                };
                for length in alternatives {
                    if next.len() >= MAX_PATTERN_ALTERNATIVE_BRANCHES {
                        return Err(unsupported(
                            "query.pattern",
                            format!(
                                "pattern alternatives expand to more than {MAX_PATTERN_ALTERNATIVE_BRANCHES} branches; simplify the pattern or split the query explicitly"
                            ),
                        ));
                    }
                    let mut variant = query.clone();
                    apply_bounded_relationship_range_alternative(&mut variant, &site, length)?;
                    next.push(variant);
                }
                continue;
            }

            next.push(query);
        }
        expanded = next;
        if !progressed {
            return Ok(expanded);
        }
    }
}

fn first_static_label_type_alternative_site(
    single_query: &SingleQuery,
) -> Option<StaticLabelTypeAlternativeSite> {
    match &single_query.kind {
        SingleQueryKind::SinglePart(single_part) => {
            first_single_part_static_label_type_alternative_site(single_part)
        }
        SingleQueryKind::MultiPart(multi_part) => {
            first_multi_part_static_label_type_alternative_site(multi_part)
        }
    }
}

fn first_single_part_static_label_type_alternative_site(
    single_part: &SinglePartQuery,
) -> Option<StaticLabelTypeAlternativeSite> {
    first_reading_clause_static_label_type_alternative_site(&single_part.reading_clauses).map(
        |(reading_clause_index, pattern_part_index, target, alternatives)| {
            StaticLabelTypeAlternativeSite::SinglePart {
                reading_clause_index,
                pattern_part_index,
                target,
                alternatives,
            }
        },
    )
}

fn first_multi_part_static_label_type_alternative_site(
    multi_part: &MultiPartQuery,
) -> Option<StaticLabelTypeAlternativeSite> {
    for (part_index, part) in multi_part.parts.iter().enumerate() {
        if let Some((reading_clause_index, pattern_part_index, target, alternatives)) =
            first_reading_clause_static_label_type_alternative_site(&part.reading_clauses)
        {
            return Some(StaticLabelTypeAlternativeSite::MultiPart {
                query_part: MultiPartAlternativePart::Part(part_index),
                reading_clause_index,
                pattern_part_index,
                target,
                alternatives,
            });
        }
    }
    first_reading_clause_static_label_type_alternative_site(&multi_part.final_part.reading_clauses)
        .map(
            |(reading_clause_index, pattern_part_index, target, alternatives)| {
                StaticLabelTypeAlternativeSite::MultiPart {
                    query_part: MultiPartAlternativePart::FinalPart,
                    reading_clause_index,
                    pattern_part_index,
                    target,
                    alternatives,
                }
            },
        )
}

fn first_reading_clause_static_label_type_alternative_site(
    reading_clauses: &[ReadingClause],
) -> Option<(
    usize,
    usize,
    PatternAlternativeTarget,
    Vec<LabelTypeAlternative>,
)> {
    for (reading_clause_index, clause) in reading_clauses.iter().enumerate() {
        let ReadingClause::Match(match_clause) = clause else {
            continue;
        };
        if let Some((pattern_part_index, target, alternatives)) =
            first_match_static_label_type_alternative_site(match_clause)
        {
            return Some((
                reading_clause_index,
                pattern_part_index,
                target,
                alternatives,
            ));
        }
    }
    None
}

fn first_match_static_label_type_alternative_site(
    match_clause: &Match,
) -> Option<(usize, PatternAlternativeTarget, Vec<LabelTypeAlternative>)> {
    for (part_index, pattern_part) in match_clause.pattern.parts.iter().enumerate() {
        let PatternElement::Path { start, chains } = &pattern_part.anonymous.element else {
            continue;
        };
        let raw_alternatives = label_expression_list_alternatives(&start.labels);
        if raw_alternatives.len() > 1 {
            let alternatives = deduplicate_node_label_alternatives(raw_alternatives);
            return Some((
                part_index,
                PatternAlternativeTarget::StartNode,
                alternatives
                    .into_iter()
                    .map(LabelTypeAlternative::NodeLabels)
                    .collect(),
            ));
        }
        for (chain_index, chain) in chains.iter().enumerate() {
            if let Some(types) = chain
                .relationship
                .detail
                .as_ref()
                .and_then(|detail| detail.types.as_ref())
            {
                let raw_alternatives = label_expression_alternatives(types);
                if raw_alternatives.len() > 1 {
                    let alternatives = deduplicate_relationship_type_alternatives(raw_alternatives);
                    return Some((
                        part_index,
                        PatternAlternativeTarget::Relationship(chain_index),
                        alternatives
                            .into_iter()
                            .map(LabelTypeAlternative::RelationshipType)
                            .collect(),
                    ));
                }
            }
            let raw_alternatives = label_expression_list_alternatives(&chain.node.labels);
            if raw_alternatives.len() > 1 {
                let alternatives = deduplicate_node_label_alternatives(raw_alternatives);
                return Some((
                    part_index,
                    PatternAlternativeTarget::ChainNode(chain_index),
                    alternatives
                        .into_iter()
                        .map(LabelTypeAlternative::NodeLabels)
                        .collect(),
                ));
            }
        }
    }
    None
}

fn deduplicate_node_label_alternatives(
    alternatives: Vec<Vec<LabelExpression>>,
) -> Vec<Vec<LabelExpression>> {
    let mut seen = BTreeSet::new();
    alternatives
        .into_iter()
        .filter(|alternative| {
            let Ok(label) = single_static_label(alternative, "query.pattern.alternative") else {
                return true;
            };
            seen.insert(label)
        })
        .collect()
}

fn deduplicate_relationship_type_alternatives(
    alternatives: Vec<LabelExpression>,
) -> Vec<LabelExpression> {
    let mut seen = BTreeSet::new();
    alternatives
        .into_iter()
        .filter(|alternative| {
            let Ok(relationship_type) = single_static_label(
                std::slice::from_ref(alternative),
                "query.pattern.alternative",
            ) else {
                return true;
            };
            seen.insert(relationship_type)
        })
        .collect()
}

fn label_expression_list_alternatives(labels: &[LabelExpression]) -> Vec<Vec<LabelExpression>> {
    let mut variants = vec![Vec::new()];
    for label in labels {
        let label_alternatives = label_expression_alternatives(label);
        let mut next = Vec::with_capacity(variants.len() * label_alternatives.len());
        for variant in &variants {
            for label_alternative in &label_alternatives {
                let mut next_variant = variant.clone();
                next_variant.push(label_alternative.clone());
                next.push(next_variant);
            }
        }
        variants = next;
    }
    variants
}

fn label_expression_alternatives(expression: &LabelExpression) -> Vec<LabelExpression> {
    match expression {
        LabelExpression::Or { lhs, rhs, .. } => label_expression_alternatives(lhs)
            .into_iter()
            .chain(label_expression_alternatives(rhs))
            .collect(),
        LabelExpression::And { lhs, rhs, span } => {
            let lhs_alternatives = label_expression_alternatives(lhs);
            let rhs_alternatives = label_expression_alternatives(rhs);
            let mut alternatives =
                Vec::with_capacity(lhs_alternatives.len() * rhs_alternatives.len());
            for lhs_alternative in &lhs_alternatives {
                for rhs_alternative in &rhs_alternatives {
                    alternatives.push(LabelExpression::And {
                        lhs: Box::new(lhs_alternative.clone()),
                        rhs: Box::new(rhs_alternative.clone()),
                        span: *span,
                    });
                }
            }
            alternatives
        }
        LabelExpression::Group { inner, .. } => label_expression_alternatives(inner),
        LabelExpression::Static(_)
        | LabelExpression::Dynamic { .. }
        | LabelExpression::Not { .. } => {
            vec![expression.clone()]
        }
    }
}

fn apply_static_label_type_alternative(
    single_query: &mut SingleQuery,
    site: &StaticLabelTypeAlternativeSite,
    alternative: LabelTypeAlternative,
) -> Result<(), CoreError> {
    match site {
        StaticLabelTypeAlternativeSite::SinglePart {
            reading_clause_index,
            pattern_part_index,
            target,
            ..
        } => {
            let SingleQueryKind::SinglePart(single_part) = &mut single_query.kind else {
                return Err(CoreError::internal(
                    "single-part label/type alternative site applied to multi-part query",
                ));
            };
            apply_reading_clause_static_label_type_alternative(
                &mut single_part.reading_clauses,
                *reading_clause_index,
                *pattern_part_index,
                *target,
                alternative,
            )
        }
        StaticLabelTypeAlternativeSite::MultiPart {
            query_part,
            reading_clause_index,
            pattern_part_index,
            target,
            ..
        } => {
            let SingleQueryKind::MultiPart(multi_part) = &mut single_query.kind else {
                return Err(CoreError::internal(
                    "multi-part label/type alternative site applied to single-part query",
                ));
            };
            let reading_clauses = match query_part {
                MultiPartAlternativePart::Part(index) => multi_part
                    .parts
                    .get_mut(*index)
                    .map(|part| &mut part.reading_clauses),
                MultiPartAlternativePart::FinalPart => {
                    Some(&mut multi_part.final_part.reading_clauses)
                }
            }
            .ok_or_else(|| CoreError::internal("multi-part alternative site is out of bounds"))?;
            apply_reading_clause_static_label_type_alternative(
                reading_clauses,
                *reading_clause_index,
                *pattern_part_index,
                *target,
                alternative,
            )
        }
    }
}

fn apply_reading_clause_static_label_type_alternative(
    reading_clauses: &mut [ReadingClause],
    reading_clause_index: usize,
    pattern_part_index: usize,
    target: PatternAlternativeTarget,
    alternative: LabelTypeAlternative,
) -> Result<(), CoreError> {
    let ReadingClause::Match(match_clause) = reading_clauses
        .get_mut(reading_clause_index)
        .ok_or_else(|| {
            CoreError::internal("label/type alternative reading clause is out of bounds")
        })?
    else {
        return Err(CoreError::internal(
            "label/type alternative site did not point at a MATCH clause",
        ));
    };
    let pattern_part = match_clause
        .pattern
        .parts
        .get_mut(pattern_part_index)
        .ok_or_else(|| {
            CoreError::internal("label/type alternative pattern part is out of bounds")
        })?;
    let PatternElement::Path { start, chains } = &mut pattern_part.anonymous.element else {
        return Err(CoreError::internal(
            "label/type alternative site did not point at a path pattern",
        ));
    };
    match (target, alternative) {
        (PatternAlternativeTarget::StartNode, LabelTypeAlternative::NodeLabels(labels)) => {
            start.labels = labels;
            Ok(())
        }
        (PatternAlternativeTarget::ChainNode(index), LabelTypeAlternative::NodeLabels(labels)) => {
            let chain = chains
                .get_mut(index)
                .ok_or_else(|| CoreError::internal("node alternative chain is out of bounds"))?;
            chain.node.labels = labels;
            Ok(())
        }
        (
            PatternAlternativeTarget::Relationship(index),
            LabelTypeAlternative::RelationshipType(relationship_type),
        ) => {
            let chain = chains.get_mut(index).ok_or_else(|| {
                CoreError::internal("relationship alternative chain is out of bounds")
            })?;
            let detail =
                chain.relationship.detail.as_mut().ok_or_else(|| {
                    CoreError::internal("relationship alternative detail is missing")
                })?;
            detail.types = Some(relationship_type);
            Ok(())
        }
        _ => Err(CoreError::internal(
            "label/type alternative site and replacement kind did not match",
        )),
    }
}

fn first_bounded_relationship_range_site(
    single_query: &SingleQuery,
) -> Result<Option<BoundedRelationshipRangeSite>, CoreError> {
    match &single_query.kind {
        SingleQueryKind::SinglePart(single_part) => {
            first_single_part_bounded_relationship_range_site(single_part)
        }
        SingleQueryKind::MultiPart(multi_part) => {
            first_multi_part_bounded_relationship_range_site(multi_part)
        }
    }
}

fn first_single_part_bounded_relationship_range_site(
    single_part: &SinglePartQuery,
) -> Result<Option<BoundedRelationshipRangeSite>, CoreError> {
    first_reading_clause_bounded_relationship_range_site(&single_part.reading_clauses).map(|site| {
        site.map(
            |(reading_clause_index, pattern_part_index, chain_index, target, alternatives)| {
                BoundedRelationshipRangeSite::SinglePart {
                    reading_clause_index,
                    pattern_part_index,
                    chain_index,
                    target,
                    alternatives,
                }
            },
        )
    })
}

fn first_multi_part_bounded_relationship_range_site(
    multi_part: &MultiPartQuery,
) -> Result<Option<BoundedRelationshipRangeSite>, CoreError> {
    for (part_index, part) in multi_part.parts.iter().enumerate() {
        if let Some((reading_clause_index, pattern_part_index, chain_index, target, alternatives)) =
            first_reading_clause_bounded_relationship_range_site(&part.reading_clauses)?
        {
            return Ok(Some(BoundedRelationshipRangeSite::MultiPart {
                query_part: MultiPartAlternativePart::Part(part_index),
                reading_clause_index,
                pattern_part_index,
                chain_index,
                target,
                alternatives,
            }));
        }
    }
    first_reading_clause_bounded_relationship_range_site(&multi_part.final_part.reading_clauses)
        .map(|site| {
            site.map(
                |(reading_clause_index, pattern_part_index, chain_index, target, alternatives)| {
                    BoundedRelationshipRangeSite::MultiPart {
                        query_part: MultiPartAlternativePart::FinalPart,
                        reading_clause_index,
                        pattern_part_index,
                        chain_index,
                        target,
                        alternatives,
                    }
                },
            )
        })
}

fn first_reading_clause_bounded_relationship_range_site(
    reading_clauses: &[ReadingClause],
) -> Result<Option<BoundedRelationshipRangeSiteInfo>, CoreError> {
    for (reading_clause_index, clause) in reading_clauses.iter().enumerate() {
        let ReadingClause::Match(match_clause) = clause else {
            continue;
        };
        if let Some((pattern_part_index, chain_index, target, alternatives)) =
            first_match_bounded_relationship_range_site(match_clause)?
        {
            if match_clause.optional {
                return Err(unsupported(
                    format!(
                        "match.reading_clauses[{reading_clause_index}].pattern.parts[{pattern_part_index}].relationships[{chain_index}]"
                    ),
                    "OPTIONAL MATCH with bounded variable-length relationship ranges is not supported yet because branch expansion would duplicate unmatched null rows",
                ));
            }
            return Ok(Some((
                reading_clause_index,
                pattern_part_index,
                chain_index,
                target,
                alternatives,
            )));
        }
    }
    Ok(None)
}

fn first_match_bounded_relationship_range_site(
    match_clause: &Match,
) -> Result<Option<MatchBoundedRelationshipRangeSiteInfo>, CoreError> {
    for (part_index, pattern_part) in match_clause.pattern.parts.iter().enumerate() {
        let PatternElement::Path { chains, .. } = &pattern_part.anonymous.element else {
            continue;
        };
        for (chain_index, chain) in chains.iter().enumerate() {
            if let Some((target, alternatives)) = bounded_relationship_range_alternatives(
                &chain.relationship,
                format!("match.pattern.parts[{part_index}].relationships[{chain_index}]"),
            )? {
                return Ok(Some((part_index, chain_index, target, alternatives)));
            }
        }
    }
    Ok(None)
}

fn bounded_relationship_range_alternatives(
    pattern: &CypherRelationshipPattern,
    path: impl Into<String>,
) -> Result<Option<(RelationshipRangeTarget, Vec<usize>)>, CoreError> {
    let path = path.into();
    let detail_range = pattern
        .detail
        .as_ref()
        .and_then(|detail| detail.range.as_ref());
    if detail_range.is_some() && pattern.quantifier.is_some() {
        return Err(unsupported(
            path,
            "relationship patterns cannot combine a variable-length range and a GQL quantifier",
        ));
    }
    if let Some(range) = detail_range {
        return bounded_range_alternatives(
            range.start,
            range.end,
            format!("{path}.range"),
            "variable-length relationship ranges require finite positive bounds such as *1..3; zero-hop and unbounded ranges are not supported yet",
        )
        .map(|alternatives| alternatives.map(|alternatives| (RelationshipRangeTarget::DetailRange, alternatives)));
    }
    if let Some(quantifier) = pattern.quantifier.as_ref() {
        return bounded_range_alternatives(
            quantifier.start,
            quantifier.end,
            format!("{path}.quantifier"),
            "relationship quantifiers require finite positive bounds such as {1,3}; zero-hop and unbounded quantifiers are not supported yet",
        )
        .map(|alternatives| alternatives.map(|alternatives| (RelationshipRangeTarget::Quantifier, alternatives)));
    }
    Ok(None)
}

fn bounded_range_alternatives(
    start: Option<i64>,
    end: Option<i64>,
    path: impl Into<String>,
    message: &'static str,
) -> Result<Option<Vec<usize>>, CoreError> {
    let path = path.into();
    let (Some(start), Some(end)) = (start, end) else {
        return Err(unsupported(path, message));
    };
    if start == end {
        return Ok(None);
    }
    if start < 1 || end < 1 || start > end {
        return Err(unsupported(path, message));
    }
    let start = usize::try_from(start).map_err(|error| {
        unsupported(
            path.clone(),
            format!("bounded relationship range lower bound is out of range: {error}"),
        )
    })?;
    let end = usize::try_from(end).map_err(|error| {
        unsupported(
            path.clone(),
            format!("bounded relationship range upper bound is out of range: {error}"),
        )
    })?;
    if end > MAX_FIXED_RELATIONSHIP_LENGTH {
        return Err(unsupported(
            path,
            format!(
                "bounded relationship range upper bound {end} exceeds Coral's current maximum of {MAX_FIXED_RELATIONSHIP_LENGTH} hops"
            ),
        ));
    }
    Ok(Some((start..=end).collect()))
}

fn apply_bounded_relationship_range_alternative(
    single_query: &mut SingleQuery,
    site: &BoundedRelationshipRangeSite,
    length: usize,
) -> Result<(), CoreError> {
    match site {
        BoundedRelationshipRangeSite::SinglePart {
            reading_clause_index,
            pattern_part_index,
            chain_index,
            target,
            ..
        } => {
            let SingleQueryKind::SinglePart(single_part) = &mut single_query.kind else {
                return Err(CoreError::internal(
                    "single-part bounded range site applied to multi-part query",
                ));
            };
            apply_reading_clause_bounded_relationship_range_alternative(
                &mut single_part.reading_clauses,
                *reading_clause_index,
                *pattern_part_index,
                *chain_index,
                *target,
                length,
            )
        }
        BoundedRelationshipRangeSite::MultiPart {
            query_part,
            reading_clause_index,
            pattern_part_index,
            chain_index,
            target,
            ..
        } => {
            let SingleQueryKind::MultiPart(multi_part) = &mut single_query.kind else {
                return Err(CoreError::internal(
                    "multi-part bounded range site applied to single-part query",
                ));
            };
            let reading_clauses = match query_part {
                MultiPartAlternativePart::Part(index) => multi_part
                    .parts
                    .get_mut(*index)
                    .map(|part| &mut part.reading_clauses),
                MultiPartAlternativePart::FinalPart => {
                    Some(&mut multi_part.final_part.reading_clauses)
                }
            }
            .ok_or_else(|| CoreError::internal("multi-part bounded range site is out of bounds"))?;
            apply_reading_clause_bounded_relationship_range_alternative(
                reading_clauses,
                *reading_clause_index,
                *pattern_part_index,
                *chain_index,
                *target,
                length,
            )
        }
    }
}

fn apply_reading_clause_bounded_relationship_range_alternative(
    reading_clauses: &mut [ReadingClause],
    reading_clause_index: usize,
    pattern_part_index: usize,
    chain_index: usize,
    target: RelationshipRangeTarget,
    length: usize,
) -> Result<(), CoreError> {
    let ReadingClause::Match(match_clause) = reading_clauses
        .get_mut(reading_clause_index)
        .ok_or_else(|| CoreError::internal("bounded range reading clause is out of bounds"))?
    else {
        return Err(CoreError::internal(
            "bounded range site did not point at a MATCH clause",
        ));
    };
    let pattern_part = match_clause
        .pattern
        .parts
        .get_mut(pattern_part_index)
        .ok_or_else(|| CoreError::internal("bounded range pattern part is out of bounds"))?;
    let PatternElement::Path { chains, .. } = &mut pattern_part.anonymous.element else {
        return Err(CoreError::internal(
            "bounded range site did not point at a path pattern",
        ));
    };
    let chain = chains
        .get_mut(chain_index)
        .ok_or_else(|| CoreError::internal("bounded range chain is out of bounds"))?;
    set_exact_relationship_range(&mut chain.relationship, target, length)
}

fn set_exact_relationship_range(
    relationship: &mut CypherRelationshipPattern,
    target: RelationshipRangeTarget,
    length: usize,
) -> Result<(), CoreError> {
    let length = i64::try_from(length)
        .map_err(|error| CoreError::internal(format!("range length out of range: {error}")))?;
    match target {
        RelationshipRangeTarget::DetailRange => {
            let detail = relationship
                .detail
                .as_mut()
                .ok_or_else(|| CoreError::internal("bounded range relationship detail missing"))?;
            let range = detail
                .range
                .as_mut()
                .ok_or_else(|| CoreError::internal("bounded relationship range missing"))?;
            set_exact_range_literal(range, length);
            Ok(())
        }
        RelationshipRangeTarget::Quantifier => {
            let quantifier = relationship
                .quantifier
                .as_mut()
                .ok_or_else(|| CoreError::internal("bounded relationship quantifier missing"))?;
            set_exact_quantifier(quantifier, length);
            Ok(())
        }
    }
}

fn set_exact_range_literal(range: &mut RangeLiteral, length: i64) {
    range.start = Some(length);
    range.end = Some(length);
}

fn set_exact_quantifier(quantifier: &mut Quantifier, length: i64) {
    quantifier.start = Some(length);
    quantifier.end = Some(length);
}

fn validate_static_label_type_alternative_expansion_supported(
    single_query: &SingleQuery,
    path: &str,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    match &single_query.kind {
        SingleQueryKind::SinglePart(single_part) => {
            validate_single_part_static_label_type_alternative_expansion_supported(
                single_part,
                path,
                context,
            )
        }
        SingleQueryKind::MultiPart(multi_part) => {
            validate_multi_part_static_label_type_alternative_expansion_supported(
                multi_part, path, context,
            )
        }
    }
}

fn validate_single_part_static_label_type_alternative_expansion_supported(
    single_part: &SinglePartQuery,
    path: &str,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    let return_clause = return_clause_from_single_part(single_part, path)?;
    validate_return_allows_static_label_type_alternative_expansion(return_clause, path, context)
}

fn validate_multi_part_static_label_type_alternative_expansion_supported(
    multi_part: &MultiPartQuery,
    path: &str,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    for (index, part) in multi_part.parts.iter().enumerate() {
        if !part.updating_clauses.is_empty() {
            return Err(unsupported(
                format!("{path}.parts[{index}].updating_clauses"),
                "write clauses are not supported by Coral virtual graphs",
            ));
        }
        validate_with_allows_static_label_type_alternative_expansion(
            &part.with,
            &format!("{path}.parts[{index}].with"),
        )?;
    }
    validate_single_part_static_label_type_alternative_expansion_supported(
        &multi_part.final_part,
        &format!("{path}.final_part"),
        context,
    )
}

fn validate_with_allows_static_label_type_alternative_expansion(
    with: &With,
    path: &str,
) -> Result<(), CoreError> {
    if with.distinct {
        return Err(unsupported(
            format!("{path}.distinct"),
            "static label/type alternatives with WITH DISTINCT require staged query planning and are not supported yet",
        ));
    }
    if with.order.is_some() || with.skip.is_some() || with.limit.is_some() {
        return Err(unsupported(
            path,
            "static label/type alternatives with WITH ORDER BY, SKIP, or LIMIT require staged query planning and are not supported yet",
        ));
    }
    for (index, item) in with.items.iter().enumerate() {
        if expression_contains_aggregate(&item.expression) {
            return Err(unsupported(
                format!("{path}.items[{index}].expression"),
                "static label/type alternatives with aggregate WITH projections require staged query planning and are not supported yet",
            ));
        }
    }
    Ok(())
}

fn validate_return_allows_static_label_type_alternative_expansion(
    return_clause: &Return,
    path: &str,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    for (index, item) in return_clause.items.iter().enumerate() {
        if count_star_item_alias(item).is_none()
            && compile_static_alternative_outer_aggregate_item(item, index, path, context)?
                .is_none()
            && expression_contains_aggregate(&item.expression)
        {
            return Err(unsupported(
                format!("{path}.return.items[{index}].expression"),
                "static label/type alternatives with property or non-count aggregate RETURN projections require staged query planning and are not supported yet",
            ));
        }
    }
    Ok(())
}

fn expression_contains_aggregate(expression: &Expression) -> bool {
    match expression {
        Expression::CountStar { .. }
        | Expression::CountSubquery(_)
        | Expression::CollectSubquery(_) => true,
        Expression::FunctionCall(function) => {
            compile_aggregate_function(function).is_some()
                || function.arguments.iter().any(expression_contains_aggregate)
        }
        Expression::Literal(literal) => literal_contains_aggregate(literal),
        Expression::PropertyLookup { base, .. }
        | Expression::IsNull { operand: base, .. }
        | Expression::UnaryOp { operand: base, .. }
        | Expression::Parenthesized(base) => expression_contains_aggregate(base),
        Expression::NodeLabels { base, labels, .. } => {
            expression_contains_aggregate(base)
                || labels.iter().any(label_expression_contains_aggregate)
        }
        Expression::BinaryOp { lhs, rhs, .. } | Expression::In { lhs, rhs, .. } => {
            expression_contains_aggregate(lhs) || expression_contains_aggregate(rhs)
        }
        Expression::Comparison { lhs, operators, .. } => {
            expression_contains_aggregate(lhs)
                || operators
                    .iter()
                    .any(|(_, rhs)| expression_contains_aggregate(rhs))
        }
        Expression::ListIndex { list, index, .. } => {
            expression_contains_aggregate(list) || expression_contains_aggregate(index)
        }
        Expression::ListSlice {
            list, start, end, ..
        } => {
            expression_contains_aggregate(list)
                || start.as_deref().is_some_and(expression_contains_aggregate)
                || end.as_deref().is_some_and(expression_contains_aggregate)
        }
        Expression::Case(case) => case_contains_aggregate(case),
        Expression::ListComprehension(comprehension) => {
            list_comprehension_contains_aggregate(comprehension)
        }
        Expression::PatternComprehension(comprehension) => {
            pattern_comprehension_contains_aggregate(comprehension)
        }
        Expression::All(filter)
        | Expression::Any(filter)
        | Expression::None(filter)
        | Expression::Single(filter) => filter_expression_contains_aggregate(filter),
        Expression::Exists(exists) => exists_expression_contains_aggregate(exists),
        Expression::MapProjection(map) => map_projection_contains_aggregate(map),
        Expression::Variable(_) | Expression::Parameter(_) | Expression::Pattern(_) => false,
    }
}

fn literal_contains_aggregate(literal: &CypherLiteral) -> bool {
    match literal {
        CypherLiteral::List(list) => list.elements.iter().any(expression_contains_aggregate),
        CypherLiteral::Map(map) => map
            .entries
            .iter()
            .any(|(_, value)| expression_contains_aggregate(value)),
        CypherLiteral::Number(_)
        | CypherLiteral::String(_)
        | CypherLiteral::Boolean(_)
        | CypherLiteral::Null => false,
    }
}

fn case_contains_aggregate(case: &CaseExpression) -> bool {
    case.scrutinee
        .as_deref()
        .is_some_and(expression_contains_aggregate)
        || case.alternatives.iter().any(|alternative| {
            expression_contains_aggregate(&alternative.when)
                || expression_contains_aggregate(&alternative.then)
        })
        || case
            .default
            .as_deref()
            .is_some_and(expression_contains_aggregate)
}

fn list_comprehension_contains_aggregate(
    comprehension: &decypher::ast::expr::ListComprehension,
) -> bool {
    comprehension
        .filter
        .as_deref()
        .is_some_and(expression_contains_aggregate)
        || comprehension
            .map
            .as_ref()
            .is_some_and(expression_contains_aggregate)
}

fn pattern_comprehension_contains_aggregate(
    comprehension: &decypher::ast::expr::PatternComprehension,
) -> bool {
    comprehension
        .where_clause
        .as_ref()
        .is_some_and(expression_contains_aggregate)
        || expression_contains_aggregate(&comprehension.map)
}

fn filter_expression_contains_aggregate(filter: &decypher::ast::expr::FilterExpression) -> bool {
    expression_contains_aggregate(&filter.collection)
        || filter
            .predicate
            .as_deref()
            .is_some_and(expression_contains_aggregate)
}

fn exists_expression_contains_aggregate(exists: &decypher::ast::expr::ExistsExpression) -> bool {
    match exists.inner.as_ref() {
        decypher::ast::expr::ExistsInner::Pattern(_, predicate) => predicate
            .as_deref()
            .is_some_and(expression_contains_aggregate),
        decypher::ast::expr::ExistsInner::RegularQuery(_) => true,
    }
}

fn map_projection_contains_aggregate(map: &decypher::ast::expr::MapProjection) -> bool {
    map.items.iter().any(|item| match item {
        decypher::ast::expr::MapProjectionItem::Literal { value, .. } => {
            expression_contains_aggregate(value)
        }
        decypher::ast::expr::MapProjectionItem::AllProperties { .. }
        | decypher::ast::expr::MapProjectionItem::PropertyLookup { .. } => false,
    })
}

fn label_expression_contains_aggregate(expression: &LabelExpression) -> bool {
    match expression {
        LabelExpression::Dynamic {
            expression: dynamic,
            ..
        } => expression_contains_aggregate(dynamic),
        LabelExpression::Or { lhs, rhs, .. } | LabelExpression::And { lhs, rhs, .. } => {
            label_expression_contains_aggregate(lhs) || label_expression_contains_aggregate(rhs)
        }
        LabelExpression::Not { inner, .. } | LabelExpression::Group { inner, .. } => {
            label_expression_contains_aggregate(inner)
        }
        LabelExpression::Static(_) => false,
    }
}

fn compile_single_part(
    query: &SinglePartQuery,
    context: &CypherCompileContext,
) -> Result<GraphPlan, CoreError> {
    let return_clause = return_clause_from_single_part(query, "query")?;

    let mut plan = GraphPlan::default();
    let mut state = CypherCompileState::default();
    compile_reading_clauses_into(
        &query.reading_clauses,
        "match",
        &mut plan,
        &mut state,
        context,
    )?;
    compile_return(return_clause, &mut plan, &state, context)?;
    reject_ignored_path_variable_references(&plan, &state, "return")?;
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
    let mut state = CypherCompileState::default();
    compile_reading_clauses_into(
        &part.reading_clauses,
        "parts[0].match",
        &mut plan,
        &mut state,
        context,
    )?;

    compile_terminal_with_clause(&part.with, &mut plan, &state, context)?;
    apply_terminal_return_projection_aliases(return_clause, &mut plan.projections)?;
    apply_terminal_return_modifiers(return_clause, &mut plan, context)?;
    reject_ignored_path_variable_references(&plan, &state, "with")?;
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
    let mut state = CypherCompileState::default();
    compile_reading_clauses_into(
        &part.reading_clauses,
        "parts[0].match",
        &mut plan,
        &mut state,
        context,
    )?;
    if let Some(predicate) =
        apply_transparent_with_scope(&part.with, &mut plan, &mut state, "parts[0].with", context)?
    {
        append_predicate_expression(predicate, &mut plan);
    }
    apply_terminal_graph_with_modifiers(&part.with, &mut plan, context)?;
    compile_return(return_clause, &mut plan, &state, context)?;
    reject_ignored_path_variable_references(&plan, &state, "return")?;
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
                nulls: None,
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
    state: &CypherCompileState,
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

    let mut aliases = BTreeSet::new();
    for (index, item) in with.items.iter().enumerate() {
        let Some(alias) = item.alias.as_ref().map(variable_name) else {
            return Err(unsupported(
                format!("with.items[{index}].alias"),
                "terminal WITH projections require explicit aliases",
            ));
        };
        if !aliases.insert(alias.clone()) {
            return Err(unsupported(
                format!("with.items[{index}].alias"),
                format!("terminal WITH projection alias '{alias}' is defined more than once"),
            ));
        }
        if matches!(&item.expression, Expression::Variable(_)) {
            return Err(unsupported(
                format!("with.items[{index}].expression"),
                "terminal WITH projections support graph properties and aggregates, not graph variable aliases",
            ));
        }
        let projection =
            compile_projection(item, format!("with.items[{index}]"), context, plan, state)?;
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
                nulls: None,
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
    projections: &mut Vec<Projection>,
) -> Result<(), CoreError> {
    if return_clause.star {
        if return_clause.items.is_empty() {
            return Ok(());
        }
        return Err(unsupported(
            "final_part.return.star",
            "RETURN * mixed with explicit projections after WITH requires scoped query planning and is not supported yet",
        ));
    }
    if return_clause.items.len() != projections.len() {
        return Err(unsupported(
            "final_part.return.items",
            "terminal RETURN after WITH must pass through every WITH alias",
        ));
    }
    let mut reordered = Vec::with_capacity(projections.len());
    let mut available = projections.clone();
    let mut returned_aliases = BTreeSet::new();
    for (index, item) in return_clause.items.iter().enumerate() {
        let Expression::Variable(variable) = &item.expression else {
            return Err(unsupported(
                format!("final_part.return.items[{index}].expression"),
                "terminal RETURN after WITH must project WITH aliases",
            ));
        };
        let alias = variable_name(variable);
        if !returned_aliases.insert(alias.clone()) {
            return Err(unsupported(
                format!("final_part.return.items[{index}].expression"),
                format!("terminal RETURN projects WITH alias '{alias}' more than once"),
            ));
        }
        let position = available
            .iter()
            .position(|projection| projection_output_alias(projection) == Some(alias.as_str()))
            .ok_or_else(|| {
                unsupported(
                    format!("final_part.return.items[{index}].expression"),
                    format!("terminal RETURN references unknown WITH alias '{alias}'"),
                )
            })?;
        let mut projection = available.remove(position);
        if let Some(alias) = &item.alias {
            set_projection_output_alias(&mut projection, variable_name(alias));
        }
        reordered.push(projection);
    }
    *projections = reordered;
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
                nulls: None,
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
    let mut state = CypherCompileState::default();
    for (index, part) in query.parts.iter().enumerate() {
        compile_transparent_multi_part_part(part, index, &mut plan, &mut state, context)?;
    }

    match query.final_part.reading_clauses.as_slice() {
        [] => {}
        clauses => {
            compile_reading_clauses_into(
                clauses,
                "final_part.match",
                &mut plan,
                &mut state,
                context,
            )?;
        }
    }
    let return_clause = return_clause_from_single_part(&query.final_part, "final_part")?;
    compile_return(return_clause, &mut plan, &state, context)?;
    reject_ignored_path_variable_references(&plan, &state, "final_part.return")?;
    Ok(plan)
}

fn compile_transparent_multi_part_part(
    part: &MultiPartQueryPart,
    index: usize,
    plan: &mut GraphPlan,
    state: &mut CypherCompileState,
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
        state,
        context,
    )?;
    if let Some(predicate) = validate_transparent_with(
        &part.with,
        plan,
        state,
        format!("parts[{index}].with"),
        context,
    )? {
        append_predicate_expression(predicate, plan);
    }
    Ok(())
}

fn validate_transparent_with(
    with: &With,
    plan: &mut GraphPlan,
    state: &mut CypherCompileState,
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
    apply_transparent_with_scope(with, plan, state, path, context)
}

fn apply_transparent_with_scope(
    with: &With,
    plan: &mut GraphPlan,
    state: &mut CypherCompileState,
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
        if !state.path_variables.is_empty() {
            return Err(unsupported(
                format!("{path}.star"),
                "WITH * cannot carry path variables because Coral does not materialize path values yet; explicitly carry graph variables to drop path values",
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

    let visible = visible_graph_variables(plan, state);
    if !carried_inputs.is_subset(&visible) {
        return Err(unsupported(
            format!("{path}.items"),
            "transparent WITH can only carry currently visible graph variables",
        ));
    }
    let dropped_variables = visible
        .difference(&carried_inputs)
        .cloned()
        .collect::<Vec<_>>();
    let mut hidden_renames = BTreeMap::new();
    for variable in &dropped_variables {
        let hidden = fresh_hidden_graph_variable(plan, state, variable);
        renames.insert(variable.clone(), hidden.clone());
        hidden_renames.insert(hidden.clone(), hidden);
    }
    if renames.iter().any(|(from, to)| from != to) {
        rename_graph_plan_variables(plan, &renames);
        rename_hidden_graph_variables(state, &renames);
    }
    state
        .hidden_graph_variables
        .extend(hidden_renames.into_values());
    state.out_of_scope_graph_names.extend(dropped_variables);
    for variable in carried_outputs {
        state.out_of_scope_graph_names.remove(&variable);
    }

    let predicate = compile_transparent_with_where(with, plan, path.clone(), context)?;
    reject_ignored_path_variable_references(plan, state, &path)?;
    if let Some(predicate) = predicate.as_ref() {
        reject_ignored_path_variable_references_in_predicate(
            predicate,
            state,
            format!("{path}.where"),
        )?;
    }
    state.path_variables.clear();
    Ok(predicate)
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

fn visible_graph_variables(plan: &GraphPlan, state: &CypherCompileState) -> BTreeSet<String> {
    bound_graph_variables(plan)
        .difference(&state.hidden_graph_variables)
        .cloned()
        .collect()
}

fn rename_hidden_graph_variables(
    state: &mut CypherCompileState,
    renames: &BTreeMap<String, String>,
) {
    state.hidden_graph_variables = state
        .hidden_graph_variables
        .iter()
        .map(|variable| renames.get(variable).unwrap_or(variable).clone())
        .collect();
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
        ScalarExpression::Key { variable }
        | ScalarExpression::ElementId { variable }
        | ScalarExpression::GraphIdentity { variable }
        | ScalarExpression::GraphPresence { variable }
        | ScalarExpression::RelationshipType { variable, .. } => {
            rename_string(variable, renames);
        }
        ScalarExpression::Coalesce { expressions } => {
            rename_scalar_expression_list_variables(expressions, renames);
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
        _ => {
            unreachable!("unary scalar expressions handled before structural rename")
        }
    }
}

fn rename_scalar_expression_list_variables(
    expressions: &mut [ScalarExpression],
    renames: &BTreeMap<String, String>,
) {
    for expression in expressions {
        rename_scalar_expression_variables(expression, renames);
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

fn reject_ignored_path_variable_references(
    plan: &GraphPlan,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    if state.path_variables.is_empty() && state.out_of_scope_graph_names.is_empty() {
        return Ok(());
    }
    for (index, projection) in plan.projections.iter().enumerate() {
        reject_ignored_path_variable_references_in_projection(
            projection,
            state,
            format!("{path}.projections[{index}]"),
        )?;
    }
    for (index, predicate) in plan.predicates.iter().enumerate() {
        reject_ignored_path_variable_references_in_property_predicate(
            predicate,
            state,
            format!("{path}.predicates[{index}]"),
        )?;
    }
    if let Some(predicate) = &plan.predicate {
        reject_ignored_path_variable_references_in_predicate(
            predicate,
            state,
            format!("{path}.predicate"),
        )?;
    }
    for (index, optional_match) in plan.optional_matches.iter().enumerate() {
        if let Some(predicate) = &optional_match.predicate {
            reject_ignored_path_variable_references_in_predicate(
                predicate,
                state,
                format!("{path}.optional_matches[{index}].predicate"),
            )?;
        }
    }
    for (index, order_key) in plan.order_by.iter().enumerate() {
        reject_ignored_path_variable_references_in_order_expression(
            &order_key.expression,
            state,
            format!("{path}.order_by[{index}]"),
        )?;
    }
    Ok(())
}

fn reject_ignored_path_variable_references_in_projection(
    projection: &Projection,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    match projection {
        Projection::Property { property, .. } => {
            reject_ignored_path_variable_property_ref(property, state, path)
        }
        Projection::Key { variable, .. }
        | Projection::ElementId { variable, .. }
        | Projection::RelationshipType { variable, .. }
        | Projection::NodeLabels { variable, .. }
        | Projection::PropertyKeys { variable, .. } => {
            reject_ignored_path_variable(variable, state, path)
        }
        Projection::Expression { expression, .. } => {
            reject_ignored_path_variable_references_in_scalar_expression(expression, state, path)
        }
        Projection::Aggregate { target, .. } => {
            reject_ignored_path_variable_references_in_aggregate_target(target, state, path)
        }
        Projection::Literal { .. }
        | Projection::LiteralList { .. }
        | Projection::CountAll { .. } => Ok(()),
    }
}

fn reject_ignored_path_variable_references_in_aggregate_target(
    target: &AggregateTarget,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    match target {
        AggregateTarget::Property(property) => {
            reject_ignored_path_variable_property_ref(property, state, path)
        }
        AggregateTarget::VariableKey { variable } => {
            reject_ignored_path_variable(variable, state, path)
        }
    }
}

fn reject_ignored_path_variable_references_in_order_expression(
    expression: &OrderExpression,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    match expression {
        OrderExpression::Property(property) => {
            reject_ignored_path_variable_property_ref(property, state, path)
        }
        OrderExpression::Key { variable }
        | OrderExpression::ElementId { variable }
        | OrderExpression::RelationshipType { variable, .. }
        | OrderExpression::NodeLabels { variable, .. }
        | OrderExpression::PropertyKeys { variable } => {
            reject_ignored_path_variable(variable, state, path)
        }
        OrderExpression::Scalar(expression) => {
            reject_ignored_path_variable_references_in_scalar_expression(expression, state, path)
        }
        OrderExpression::Literal(_) | OrderExpression::ProjectionAlias(_) => Ok(()),
    }
}

fn reject_ignored_path_variable_references_in_predicate(
    expression: &PredicateExpression,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    match expression {
        PredicateExpression::Boolean(_) => Ok(()),
        PredicateExpression::Comparison(predicate) => {
            reject_ignored_path_variable_references_in_property_predicate(predicate, state, path)
        }
        PredicateExpression::KeyComparison(predicate) => {
            reject_ignored_path_variable(&predicate.variable, state, format!("{path}.variable"))?;
            reject_ignored_path_variable_references_in_predicate_rhs(
                &predicate.rhs,
                state,
                format!("{path}.rhs"),
            )
        }
        PredicateExpression::ElementIdComparison(predicate) => {
            reject_ignored_path_variable(&predicate.variable, state, format!("{path}.variable"))?;
            reject_ignored_path_variable_references_in_predicate_rhs(
                &predicate.rhs,
                state,
                format!("{path}.rhs"),
            )
        }
        PredicateExpression::Presence(predicate) => {
            reject_ignored_path_variable(&predicate.variable, state, format!("{path}.variable"))
        }
        PredicateExpression::PropertyKeyMembership(predicate) => {
            reject_ignored_path_variable(&predicate.variable, state, format!("{path}.variable"))
        }
        PredicateExpression::ScalarComparison(predicate) => {
            reject_ignored_path_variable_references_in_scalar_expression(
                &predicate.lhs,
                state,
                format!("{path}.lhs"),
            )?;
            reject_ignored_path_variable_references_in_scalar_predicate_rhs(
                &predicate.rhs,
                state,
                format!("{path}.rhs"),
            )
        }
        PredicateExpression::And { left, right }
        | PredicateExpression::Or { left, right }
        | PredicateExpression::Xor { left, right } => {
            reject_ignored_path_variable_references_in_predicate(
                left,
                state,
                format!("{path}.left"),
            )?;
            reject_ignored_path_variable_references_in_predicate(
                right,
                state,
                format!("{path}.right"),
            )
        }
        PredicateExpression::Not { expression } => {
            reject_ignored_path_variable_references_in_predicate(
                expression,
                state,
                format!("{path}.expression"),
            )
        }
    }
}

fn reject_ignored_path_variable_references_in_property_predicate(
    predicate: &PropertyPredicate,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    reject_ignored_path_variable_property_ref(
        &predicate.property,
        state,
        format!("{path}.property"),
    )?;
    reject_ignored_path_variable_references_in_predicate_rhs(
        &predicate.rhs,
        state,
        format!("{path}.rhs"),
    )
}

fn reject_ignored_path_variable_references_in_predicate_rhs(
    rhs: &PredicateRhs,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    match rhs {
        PredicateRhs::Property(property) => {
            reject_ignored_path_variable_property_ref(property, state, path)
        }
        PredicateRhs::Key { variable } | PredicateRhs::ElementId { variable } => {
            reject_ignored_path_variable(variable, state, path)
        }
        PredicateRhs::Literal(_) | PredicateRhs::List(_) => Ok(()),
    }
}

fn reject_ignored_path_variable_references_in_scalar_predicate_rhs(
    rhs: &ScalarPredicateRhs,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    match rhs {
        ScalarPredicateRhs::Expression(expression) => {
            reject_ignored_path_variable_references_in_scalar_expression(expression, state, path)
        }
        ScalarPredicateRhs::List(_) => Ok(()),
    }
}

fn reject_ignored_path_variable_references_in_scalar_expression(
    expression: &ScalarExpression,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    if let Some(expression) = unary_scalar_expression_operand(expression) {
        return reject_ignored_path_variable_references_in_scalar_expression(
            expression, state, path,
        );
    }

    match expression {
        ScalarExpression::Property(property) => {
            reject_ignored_path_variable_property_ref(property, state, path)
        }
        ScalarExpression::Literal(_) => Ok(()),
        ScalarExpression::Predicate(predicate) => {
            reject_ignored_path_variable_references_in_predicate(predicate, state, path)
        }
        ScalarExpression::Key { variable }
        | ScalarExpression::ElementId { variable }
        | ScalarExpression::GraphIdentity { variable }
        | ScalarExpression::GraphPresence { variable }
        | ScalarExpression::RelationshipType { variable, .. } => {
            reject_ignored_path_variable(variable, state, path)
        }
        _ => reject_ignored_path_variable_references_in_structural_scalar_expression(
            expression, state, path,
        ),
    }
}

fn reject_ignored_path_variable_references_in_structural_scalar_expression(
    expression: &ScalarExpression,
    state: &CypherCompileState,
    path: String,
) -> Result<(), CoreError> {
    match expression {
        ScalarExpression::Coalesce { expressions } => {
            reject_path_variables_in_scalar_list(expressions, state, format!("{path}.expressions"))
        }
        ScalarExpression::NullIf { expression, value } => reject_path_variables_in_scalar_pair(
            ("expression", expression),
            ("value", value),
            state,
            path,
        ),
        ScalarExpression::Round { expression, places } => {
            reject_path_variables_in_scalar_optional_pair(
                ("expression", expression),
                ("places", places.as_deref()),
                state,
                path,
            )
        }
        ScalarExpression::Left { expression, count }
        | ScalarExpression::Right { expression, count } => reject_path_variables_in_scalar_pair(
            ("expression", expression),
            ("count", count),
            state,
            path,
        ),
        ScalarExpression::Replace {
            expression,
            search,
            replacement,
        } => reject_path_variables_in_replace_expression(
            expression,
            search,
            replacement,
            state,
            path,
        ),
        ScalarExpression::Substring {
            expression,
            start,
            length,
        } => reject_path_variables_in_substring_expression(
            expression,
            start,
            length.as_deref(),
            state,
            path,
        ),
        ScalarExpression::Arithmetic { left, right, .. } => {
            reject_path_variables_in_scalar_pair(("left", left), ("right", right), state, path)
        }
        ScalarExpression::Atan2 { y, x } => {
            reject_path_variables_in_scalar_pair(("y", y), ("x", x), state, path)
        }
        ScalarExpression::Case {
            alternatives,
            else_expression,
        } => reject_ignored_path_variable_references_in_case_expression(
            alternatives,
            else_expression.as_deref(),
            state,
            path,
        ),
        _ => {
            reject_ignored_path_variable_references_in_non_structural_scalar_expression(expression)
        }
    }
}

fn reject_ignored_path_variable_references_in_non_structural_scalar_expression(
    expression: &ScalarExpression,
) -> Result<(), CoreError> {
    match expression {
        ScalarExpression::Property(_)
        | ScalarExpression::Literal(_)
        | ScalarExpression::Predicate(_)
        | ScalarExpression::Key { .. }
        | ScalarExpression::ElementId { .. }
        | ScalarExpression::GraphIdentity { .. }
        | ScalarExpression::GraphPresence { .. }
        | ScalarExpression::RelationshipType { .. } => {
            unreachable!("simple scalar expressions handled before structural path checks")
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
            unreachable!("unary scalar expressions handled before structural path checks")
        }
        ScalarExpression::Coalesce { .. }
        | ScalarExpression::NullIf { .. }
        | ScalarExpression::Round { .. }
        | ScalarExpression::Left { .. }
        | ScalarExpression::Right { .. }
        | ScalarExpression::Replace { .. }
        | ScalarExpression::Substring { .. }
        | ScalarExpression::Arithmetic { .. }
        | ScalarExpression::Atan2 { .. }
        | ScalarExpression::Case { .. } => {
            unreachable!("structural scalar expressions handled before this path check")
        }
    }
}

fn reject_path_variables_in_scalar_list(
    expressions: &[ScalarExpression],
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    for (index, expression) in expressions.iter().enumerate() {
        reject_ignored_path_variable_references_in_scalar_expression(
            expression,
            state,
            format!("{path}[{index}]"),
        )?;
    }
    Ok(())
}

fn reject_path_variables_in_scalar_pair(
    left: (&str, &ScalarExpression),
    right: (&str, &ScalarExpression),
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    reject_ignored_path_variable_references_in_scalar_expression(
        left.1,
        state,
        format!("{path}.{}", left.0),
    )?;
    reject_ignored_path_variable_references_in_scalar_expression(
        right.1,
        state,
        format!("{path}.{}", right.0),
    )
}

fn reject_path_variables_in_scalar_optional_pair(
    required: (&str, &ScalarExpression),
    optional: (&str, Option<&ScalarExpression>),
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    reject_ignored_path_variable_references_in_scalar_expression(
        required.1,
        state,
        format!("{path}.{}", required.0),
    )?;
    if let Some(expression) = optional.1 {
        reject_ignored_path_variable_references_in_scalar_expression(
            expression,
            state,
            format!("{path}.{}", optional.0),
        )?;
    }
    Ok(())
}

fn reject_path_variables_in_replace_expression(
    expression: &ScalarExpression,
    search: &ScalarExpression,
    replacement: &ScalarExpression,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    reject_ignored_path_variable_references_in_scalar_expression(
        expression,
        state,
        format!("{path}.expression"),
    )?;
    reject_ignored_path_variable_references_in_scalar_expression(
        search,
        state,
        format!("{path}.search"),
    )?;
    reject_ignored_path_variable_references_in_scalar_expression(
        replacement,
        state,
        format!("{path}.replacement"),
    )
}

fn reject_path_variables_in_substring_expression(
    expression: &ScalarExpression,
    start: &ScalarExpression,
    length: Option<&ScalarExpression>,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    reject_ignored_path_variable_references_in_scalar_expression(
        expression,
        state,
        format!("{path}.expression"),
    )?;
    reject_ignored_path_variable_references_in_scalar_expression(
        start,
        state,
        format!("{path}.start"),
    )?;
    if let Some(length) = length {
        reject_ignored_path_variable_references_in_scalar_expression(
            length,
            state,
            format!("{path}.length"),
        )?;
    }
    Ok(())
}

fn reject_ignored_path_variable_references_in_case_expression(
    alternatives: &[ScalarCaseAlternative],
    else_expression: Option<&ScalarExpression>,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    for (index, alternative) in alternatives.iter().enumerate() {
        reject_ignored_path_variable_references_in_predicate(
            &alternative.when,
            state,
            format!("{path}.alternatives[{index}].when"),
        )?;
        reject_ignored_path_variable_references_in_scalar_expression(
            &alternative.then,
            state,
            format!("{path}.alternatives[{index}].then"),
        )?;
    }
    if let Some(else_expression) = else_expression {
        reject_ignored_path_variable_references_in_scalar_expression(
            else_expression,
            state,
            format!("{path}.else"),
        )?;
    }
    Ok(())
}

fn unary_scalar_expression_operand(expression: &ScalarExpression) -> Option<&ScalarExpression> {
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

fn reject_ignored_path_variable_property_ref(
    property: &PropertyRef,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    reject_ignored_path_variable(&property.variable, state, path)
}

fn reject_ignored_path_variable(
    variable: &str,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    if state.out_of_scope_graph_names.contains(variable) {
        return Err(unsupported(
            path,
            format!("graph variable '{variable}' is not in scope after WITH"),
        ));
    }
    if state.path_variables.contains_key(variable) {
        return Err(unsupported(
            path,
            format!(
                "path variable '{variable}' cannot be used as a graph value because Coral does not materialize path values yet"
            ),
        ));
    }
    Ok(())
}

fn mark_graph_variable_in_scope(state: &mut CypherCompileState, variable: &str) {
    state.out_of_scope_graph_names.remove(variable);
}

fn compile_reading_clauses_into(
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
                compile_match_into(match_clause, plan, state, context)?;
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

    if relationship_indices.len() != 1 {
        return Err(unsupported(
            path,
            "OPTIONAL MATCH currently requires a single relationship pattern to preserve whole-pattern null semantics",
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

fn compile_pattern_part_into(
    pattern_part: &PatternPart,
    part_index: usize,
    optional: bool,
    plan: &mut GraphPlan,
    state: &mut CypherCompileState,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    validate_ignored_path_variable(
        pattern_part,
        plan,
        state,
        format!("match.pattern.parts[{part_index}]"),
    )?;

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
        mark_graph_variable_in_scope(state, &pattern.variable);
        plan.nodes.push(pattern);
    }
    let mut previous_label = start_node.label.clone();

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
        let next_label = next_node.label.clone();
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
        plan.predicates.extend(next_node.predicates);
        if let Some(pattern) = next_node.pattern {
            mark_graph_variable_in_scope(state, &pattern.variable);
            plan.nodes.push(pattern);
        }
        if relationship.length == 1 {
            plan.predicates.extend(relationship.predicates);
            if let Some(variable) = relationship.pattern.variable.as_deref() {
                mark_graph_variable_in_scope(state, variable);
            }
            if optional {
                plan.optional_relationships.push(relationship_index);
            }
            plan.relationships.push(relationship.pattern);
        } else {
            if previous_label != next_label {
                return Err(unsupported(
                    format!("match.pattern.parts[{part_index}].relationships[{chain_index}]"),
                    "fixed-length relationship ranges greater than 1 currently require same-label endpoints so Coral can infer intermediate node mappings",
                ));
            }
            append_fixed_length_relationship(
                plan,
                state,
                &relationship.pattern,
                &relationship.predicates,
                relationship.length,
                &FixedLengthExpansion {
                    part_index,
                    chain_index,
                    left_variable: &previous_variable,
                    right_variable: &next_variable,
                    node_label: &previous_label,
                    optional,
                },
            );
        }
        previous_variable = next_variable;
        previous_label = next_label;
    }

    Ok(())
}

struct FixedLengthExpansion<'a> {
    part_index: usize,
    chain_index: usize,
    left_variable: &'a str,
    right_variable: &'a str,
    node_label: &'a str,
    optional: bool,
}

fn append_fixed_length_relationship(
    plan: &mut GraphPlan,
    state: &mut CypherCompileState,
    template: &RelationshipPattern,
    predicates: &[PropertyPredicate],
    length: usize,
    expansion: &FixedLengthExpansion<'_>,
) {
    let mut left = expansion.left_variable.to_string();
    for hop in 1..=length {
        let right = if hop == length {
            expansion.right_variable.to_string()
        } else {
            let variable = fresh_internal_node_variable_avoiding(
                plan,
                expansion.part_index,
                expansion.chain_index + hop,
                expansion.right_variable,
            );
            mark_graph_variable_in_scope(state, &variable);
            plan.nodes.push(NodePattern {
                variable: variable.clone(),
                label: expansion.node_label.to_string(),
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
        }
        if expansion.optional {
            plan.optional_relationships.push(relationship_index);
        }
        plan.relationships.push(pattern);
        left = right;
    }
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

fn validate_ignored_path_variable(
    pattern_part: &PatternPart,
    plan: &GraphPlan,
    state: &mut CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
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
        return Ok(());
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
    state.path_variables.insert(name, PathBinding { length });
    Ok(())
}

fn path_pattern_length(pattern_part: &PatternPart, path: &str) -> Result<usize, CoreError> {
    let PatternElement::Path { chains, .. } = &pattern_part.anonymous.element else {
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

fn anonymous_pattern_variables(pattern_part: &PatternPart) -> BTreeSet<String> {
    let PatternElement::Path { start, chains } = &pattern_part.anonymous.element else {
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
    let length = relationship_fixed_length(pattern, &path)?;

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
    if length > 1 && detail.variable.is_some() {
        return Err(unsupported(
            format!("{path}.variable"),
            "fixed-length relationship ranges greater than 1 cannot bind a relationship variable because Coral does not materialize relationship lists yet",
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
        length,
    })
}

fn relationship_fixed_length(
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
                "relationship quantifiers must be exact positive fixed lengths such as {2}",
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
                "variable-length relationship ranges must be exact positive fixed lengths such as *2 or *2..2",
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
    if start != end || start < 1 {
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

fn compile_return(
    return_clause: &Return,
    plan: &mut GraphPlan,
    state: &CypherCompileState,
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
        let projection =
            compile_projection(item, format!("return.items[{index}]"), context, plan, state)?;
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
                nulls: None,
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
        } => compile_arithmetic_order_expression(expression, path, plan, context),
        Expression::BinaryOp { .. } => {
            if let Some(expression) =
                compile_optional_boolean_scalar_expression(expression, path.clone(), plan, context)?
            {
                Ok(OrderExpression::Scalar(expression))
            } else {
                compile_arithmetic_order_expression(expression, path, plan, context)
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
                compile_scalar_function_expression_with_plan(function, path.clone(), plan, context)?
            {
                return Ok(OrderExpression::Scalar(expression));
            }
            if compile_aggregate_function(function).is_some() {
                return aggregate_order_expression_for_projection(
                    function,
                    projections,
                    path,
                    plan,
                    context,
                );
            }
            Ok(OrderExpression::Property(compile_property_ref(
                expression,
                path,
                Some(plan),
                context,
            )?))
        }
        _ => Ok(OrderExpression::Property(compile_property_ref(
            expression,
            path,
            Some(plan),
            context,
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
    plan: &GraphPlan,
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
    let target =
        compile_function_aggregate_target(function, function_kind, &path, Some(plan), context)?;
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
    let variable = compile_id_variable(function, path, plan, context)?;
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

fn compile_key_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let variable = compile_id_variable(function, path, plan, context)?;
    Ok(ScalarExpression::Key { variable })
}

fn compile_element_id_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let variable = compile_element_id_variable(function, path, plan, context)?;
    Ok(ScalarExpression::ElementId { variable })
}

fn compile_type_order_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    let ScalarExpression::RelationshipType {
        variable,
        relationship_type,
    } = compile_relationship_type_scalar_expression(function, path, plan, context)?
    else {
        unreachable!("relationship type helper returned non-relationship scalar expression");
    };
    Ok(OrderExpression::RelationshipType {
        variable,
        relationship_type,
    })
}

fn compile_relationship_type_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
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
    Ok(ScalarExpression::RelationshipType {
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
    state: &CypherCompileState,
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
            compile_arithmetic_projection(item, path, plan, context)
        }
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            ..
        }
        | Expression::BinaryOp { .. } => compile_arithmetic_projection(item, path, plan, context),
        Expression::Case(case) => compile_case_projection(case, item, path, plan, context),
        Expression::FunctionCall(function) if is_id_function(function) => {
            compile_id_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_element_id_function(function) => {
            compile_element_id_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_internal_graph_identity_function(function) => {
            compile_internal_graph_identity_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_internal_graph_presence_function(function) => {
            compile_internal_graph_presence_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_type_function(function) => {
            compile_type_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_labels_function(function) => {
            compile_labels_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_keys_function(function) => {
            compile_keys_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_length_function(function) => {
            compile_path_length_projection(function, item, path, state, context)
        }
        Expression::FunctionCall(function) => {
            if let Some(projection) =
                compile_scalar_function_projection(function, item, path.clone(), plan, context)?
            {
                return Ok(projection);
            }
            if compile_aggregate_function(function).is_some() {
                return compile_aggregate_projection(function, item, path, plan, context);
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
            property: compile_property_ref(
                expression,
                format!("{path}.expression"),
                Some(plan),
                context,
            )?,
            alias: item.alias.as_ref().map(variable_name),
        }),
    }
}

fn compile_path_length_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let variable = compile_single_variable_function_argument(
        function,
        format!("{path}.expression.arguments"),
        "length() supports exactly one path variable argument",
        context,
    )?;
    let binding = state.path_variables.get(&variable).ok_or_else(|| {
        unsupported(
            format!("{path}.expression.arguments[0]"),
            format!("length() argument '{variable}' is not a bound path variable"),
        )
    })?;
    let length = i64::try_from(binding.length)
        .map_err(|error| CoreError::internal(format!("path length overflow: {error}")))?;
    Ok(Projection::Expression {
        expression: ScalarExpression::Literal(Literal::Integer(length)),
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "length".to_string(), variable_name),
    })
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
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    Ok(Projection::Expression {
        expression: compile_scalar_expression_with_plan(
            &item.expression,
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
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<Projection>, CoreError> {
    let path = path.into();
    let Some(expression) = compile_scalar_function_expression_with_plan(
        function,
        format!("{path}.expression"),
        plan,
        context,
    )?
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
    plan: Option<&GraphPlan>,
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
            compile_scalar_expression_in_mode(
                expression,
                format!("{path}.arguments[{index}]"),
                plan,
                context,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ScalarExpression::Coalesce { expressions })
}

fn compile_null_if_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (expression, value) =
        compile_two_scalar_function_arguments(function, path, "nullIf", plan, context)?;
    Ok(ScalarExpression::NullIf {
        expression: Box::new(expression),
        value: Box::new(value),
    })
}

fn compile_to_string_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToString {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "toString", plan, context,
        )?),
    })
}

fn compile_to_integer_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToInteger {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toInteger",
            plan,
            context,
        )?),
    })
}

fn compile_to_float_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToFloat {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "toFloat", plan, context,
        )?),
    })
}

fn compile_to_boolean_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToBoolean {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toBoolean",
            plan,
            context,
        )?),
    })
}

fn compile_to_string_or_null_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToStringOrNull {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toStringOrNull",
            plan,
            context,
        )?),
    })
}

fn compile_to_integer_or_null_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToIntegerOrNull {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toIntegerOrNull",
            plan,
            context,
        )?),
    })
}

fn compile_to_float_or_null_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToFloatOrNull {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toFloatOrNull",
            plan,
            context,
        )?),
    })
}

fn compile_to_boolean_or_null_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToBooleanOrNull {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toBooleanOrNull",
            plan,
            context,
        )?),
    })
}

fn compile_to_lower_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let function_name = single_segment_function_name(function).unwrap_or("toLower");
    Ok(ScalarExpression::ToLower {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            function_name,
            plan,
            context,
        )?),
    })
}

fn compile_to_upper_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let function_name = single_segment_function_name(function).unwrap_or("toUpper");
    Ok(ScalarExpression::ToUpper {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            function_name,
            plan,
            context,
        )?),
    })
}

fn compile_trim_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let function_name = single_segment_function_name(function).unwrap_or("trim");
    Ok(ScalarExpression::Trim {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            function_name,
            plan,
            context,
        )?),
    })
}

fn compile_ltrim_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::LTrim {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "lTrim", plan, context,
        )?),
    })
}

fn compile_rtrim_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::RTrim {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "rTrim", plan, context,
        )?),
    })
}

fn compile_replace_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
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
        expression: Box::new(compile_scalar_expression_in_mode(
            expression,
            format!("{path}.arguments[0]"),
            plan,
            context,
        )?),
        search: Box::new(compile_scalar_expression_in_mode(
            search,
            format!("{path}.arguments[1]"),
            plan,
            context,
        )?),
        replacement: Box::new(compile_scalar_expression_in_mode(
            replacement,
            format!("{path}.arguments[2]"),
            plan,
            context,
        )?),
    })
}

fn compile_character_length_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let function_name = qualified_function_name(function);
    Ok(ScalarExpression::CharacterLength {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            function_name.as_str(),
            plan,
            context,
        )?),
    })
}

fn compile_substring_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match function.arguments.as_slice() {
        [expression, start] => Ok(ScalarExpression::Substring {
            expression: Box::new(compile_scalar_expression_in_mode(
                expression,
                format!("{path}.arguments[0]"),
                plan,
                context,
            )?),
            start: Box::new(compile_scalar_expression_in_mode(
                start,
                format!("{path}.arguments[1]"),
                plan,
                context,
            )?),
            length: None,
        }),
        [expression, start, length] => Ok(ScalarExpression::Substring {
            expression: Box::new(compile_scalar_expression_in_mode(
                expression,
                format!("{path}.arguments[0]"),
                plan,
                context,
            )?),
            start: Box::new(compile_scalar_expression_in_mode(
                start,
                format!("{path}.arguments[1]"),
                plan,
                context,
            )?),
            length: Some(Box::new(compile_scalar_expression_in_mode(
                length,
                format!("{path}.arguments[2]"),
                plan,
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
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (expression, count) =
        compile_two_scalar_function_arguments(function, path, "left", plan, context)?;
    Ok(ScalarExpression::Left {
        expression: Box::new(expression),
        count: Box::new(count),
    })
}

fn compile_right_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (expression, count) =
        compile_two_scalar_function_arguments(function, path, "right", plan, context)?;
    Ok(ScalarExpression::Right {
        expression: Box::new(expression),
        count: Box::new(count),
    })
}

fn compile_reverse_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Reverse {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "reverse", plan, context,
        )?),
    })
}

fn compile_abs_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Abs {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "abs", plan, context,
        )?),
    })
}

fn compile_ceil_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Ceil {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "ceil", plan, context,
        )?),
    })
}

fn compile_floor_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Floor {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "floor", plan, context,
        )?),
    })
}

fn compile_round_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match function.arguments.as_slice() {
        [expression] => Ok(ScalarExpression::Round {
            expression: Box::new(compile_scalar_expression_in_mode(
                expression,
                format!("{path}.arguments[0]"),
                plan,
                context,
            )?),
            places: None,
        }),
        [expression, places] => Ok(ScalarExpression::Round {
            expression: Box::new(compile_scalar_expression_in_mode(
                expression,
                format!("{path}.arguments[0]"),
                plan,
                context,
            )?),
            places: Some(Box::new(compile_scalar_expression_in_mode(
                places,
                format!("{path}.arguments[1]"),
                plan,
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
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Sqrt {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "sqrt", plan, context,
        )?),
    })
}

fn compile_sign_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Sign {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "sign", plan, context,
        )?),
    })
}

fn compile_exp_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Exp {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "exp", plan, context,
        )?),
    })
}

fn compile_log_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Log {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "log", plan, context,
        )?),
    })
}

fn compile_log10_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Log10 {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "log10", plan, context,
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
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Sin {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "sin", plan, context,
        )?),
    })
}

fn compile_cos_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Cos {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "cos", plan, context,
        )?),
    })
}

fn compile_tan_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Tan {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "tan", plan, context,
        )?),
    })
}

fn compile_cot_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Cot {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "cot", plan, context,
        )?),
    })
}

fn compile_asin_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Asin {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "asin", plan, context,
        )?),
    })
}

fn compile_acos_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Acos {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "acos", plan, context,
        )?),
    })
}

fn compile_atan_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Atan {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "atan", plan, context,
        )?),
    })
}

fn compile_atan2_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (y, x) = compile_two_scalar_function_arguments(function, path, "atan2", plan, context)?;
    Ok(ScalarExpression::Atan2 {
        y: Box::new(y),
        x: Box::new(x),
    })
}

fn compile_degrees_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Degrees {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "degrees", plan, context,
        )?),
    })
}

fn compile_radians_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Radians {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "radians", plan, context,
        )?),
    })
}

fn compile_haversin_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(haversin_expression(
        compile_single_scalar_function_argument(function, path, "haversin", plan, context)?,
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
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!("{function_name}() requires exactly one argument"),
        ));
    };
    compile_scalar_expression_in_mode(argument, format!("{path}.arguments[0]"), plan, context)
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
    plan: Option<&GraphPlan>,
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
        compile_scalar_expression_in_mode(left, format!("{path}.arguments[0]"), plan, context)?,
        compile_scalar_expression_in_mode(right, format!("{path}.arguments[1]"), plan, context)?,
    ))
}

fn compile_scalar_function_expression_with_plan(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    compile_scalar_function_expression_in_mode(function, path, Some(plan), context)
}

fn compile_scalar_function_expression_in_mode(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    if is_id_function(function) {
        match plan {
            Some(plan) => {
                compile_key_scalar_expression(function, path.clone(), plan, context).map(Some)
            }
            None => Err(unsupported(
                path,
                "id() scalar expressions require graph context",
            )),
        }
    } else if is_element_id_function(function) {
        match plan {
            Some(plan) => {
                compile_element_id_scalar_expression(function, path.clone(), plan, context)
                    .map(Some)
            }
            None => Err(unsupported(
                path,
                "elementId() scalar expressions require graph context",
            )),
        }
    } else if is_type_function(function) {
        match plan {
            Some(plan) => {
                compile_relationship_type_scalar_expression(function, path.clone(), plan, context)
                    .map(Some)
            }
            None => Err(unsupported(
                path,
                "type() scalar expressions require graph context",
            )),
        }
    } else if let Some(expression) =
        compile_core_scalar_function_expression(function, &path, plan, context)?
    {
        Ok(Some(expression))
    } else {
        compile_numeric_scalar_function_expression(function, &path, plan, context)
    }
}

fn compile_core_scalar_function_expression(
    function: &FunctionInvocation,
    path: &str,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let expression = if is_coalesce_function(function) {
        compile_coalesce_scalar_expression(function, path, plan, context)?
    } else if is_null_if_function(function) {
        compile_null_if_scalar_expression(function, path, plan, context)?
    } else if is_to_string_function(function) {
        compile_to_string_scalar_expression(function, path, plan, context)?
    } else if is_to_integer_function(function) {
        compile_to_integer_scalar_expression(function, path, plan, context)?
    } else if is_to_float_function(function) {
        compile_to_float_scalar_expression(function, path, plan, context)?
    } else if is_to_boolean_function(function) {
        compile_to_boolean_scalar_expression(function, path, plan, context)?
    } else if is_to_string_or_null_function(function) {
        compile_to_string_or_null_scalar_expression(function, path, plan, context)?
    } else if is_to_integer_or_null_function(function) {
        compile_to_integer_or_null_scalar_expression(function, path, plan, context)?
    } else if is_to_float_or_null_function(function) {
        compile_to_float_or_null_scalar_expression(function, path, plan, context)?
    } else if is_to_boolean_or_null_function(function) {
        compile_to_boolean_or_null_scalar_expression(function, path, plan, context)?
    } else if is_to_lower_function(function) {
        compile_to_lower_scalar_expression(function, path, plan, context)?
    } else if is_to_upper_function(function) {
        compile_to_upper_scalar_expression(function, path, plan, context)?
    } else if is_trim_function(function) {
        compile_trim_scalar_expression(function, path, plan, context)?
    } else if is_ltrim_function(function) {
        compile_ltrim_scalar_expression(function, path, plan, context)?
    } else if is_rtrim_function(function) {
        compile_rtrim_scalar_expression(function, path, plan, context)?
    } else if is_replace_function(function) {
        compile_replace_scalar_expression(function, path, plan, context)?
    } else if is_character_length_function(function) {
        compile_character_length_scalar_expression(function, path, plan, context)?
    } else if is_substring_function(function) {
        compile_substring_scalar_expression(function, path, plan, context)?
    } else if is_left_function(function) {
        compile_left_scalar_expression(function, path, plan, context)?
    } else if is_right_function(function) {
        compile_right_scalar_expression(function, path, plan, context)?
    } else if is_reverse_function(function) {
        compile_reverse_scalar_expression(function, path, plan, context)?
    } else {
        return Ok(None);
    };
    Ok(Some(expression))
}

fn compile_numeric_scalar_function_expression(
    function: &FunctionInvocation,
    path: &str,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let expression = if is_abs_function(function) {
        compile_abs_scalar_expression(function, path, plan, context)?
    } else if is_ceil_function(function) {
        compile_ceil_scalar_expression(function, path, plan, context)?
    } else if is_floor_function(function) {
        compile_floor_scalar_expression(function, path, plan, context)?
    } else if is_round_function(function) {
        compile_round_scalar_expression(function, path, plan, context)?
    } else if is_sqrt_function(function) {
        compile_sqrt_scalar_expression(function, path, plan, context)?
    } else if is_sign_function(function) {
        compile_sign_scalar_expression(function, path, plan, context)?
    } else if is_exp_function(function) {
        compile_exp_scalar_expression(function, path, plan, context)?
    } else if is_log_function(function) {
        compile_log_scalar_expression(function, path, plan, context)?
    } else if is_log10_function(function) {
        compile_log10_scalar_expression(function, path, plan, context)?
    } else if is_pi_function(function) {
        compile_pi_scalar_expression(function, path)?
    } else if is_e_function(function) {
        compile_e_scalar_expression(function, path)?
    } else if is_sin_function(function) {
        compile_sin_scalar_expression(function, path, plan, context)?
    } else if is_cos_function(function) {
        compile_cos_scalar_expression(function, path, plan, context)?
    } else if is_tan_function(function) {
        compile_tan_scalar_expression(function, path, plan, context)?
    } else if is_cot_function(function) {
        compile_cot_scalar_expression(function, path, plan, context)?
    } else if is_asin_function(function) {
        compile_asin_scalar_expression(function, path, plan, context)?
    } else if is_acos_function(function) {
        compile_acos_scalar_expression(function, path, plan, context)?
    } else if is_atan_function(function) {
        compile_atan_scalar_expression(function, path, plan, context)?
    } else if is_atan2_function(function) {
        compile_atan2_scalar_expression(function, path, plan, context)?
    } else if is_degrees_function(function) {
        compile_degrees_scalar_expression(function, path, plan, context)?
    } else if is_radians_function(function) {
        compile_radians_scalar_expression(function, path, plan, context)?
    } else if is_haversin_function(function) {
        compile_haversin_scalar_expression(function, path, plan, context)?
    } else {
        return Ok(None);
    };
    Ok(Some(expression))
}

fn compile_scalar_expression_with_plan(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    compile_scalar_expression_in_mode(expression, path, Some(plan), context)
}

fn compile_scalar_expression_in_mode(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_scalar_expression_in_mode(inner, path, plan, context)
        }
        Expression::PropertyLookup { .. } => Ok(ScalarExpression::Property(compile_property_ref(
            expression, path, plan, context,
        )?)),
        expression if is_literal_expression(expression) => Ok(ScalarExpression::Literal(
            compile_literal(expression, path, context)?,
        )),
        Expression::BinaryOp { op, lhs, rhs, .. } => Ok(ScalarExpression::Arithmetic {
            operator: compile_arithmetic_operator(*op, format!("{path}.operator"))?,
            left: Box::new(compile_scalar_expression_in_mode(
                lhs,
                format!("{path}.lhs"),
                plan,
                context,
            )?),
            right: Box::new(compile_scalar_expression_in_mode(
                rhs,
                format!("{path}.rhs"),
                plan,
                context,
            )?),
        }),
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            operand,
            ..
        } => Ok(ScalarExpression::Negate {
            expression: Box::new(compile_scalar_expression_in_mode(
                operand,
                format!("{path}.operand"),
                plan,
                context,
            )?),
        }),
        Expression::Case(case) => compile_case_scalar_expression_in_mode(
            case,
            path,
            PredicateCompileMode::CaseWhen { plan },
            context,
        ),
        Expression::FunctionCall(function) => {
            compile_scalar_function_expression_in_mode(function, path.clone(), plan, context)?
                .ok_or_else(|| {
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
    let variable = compile_id_variable(function, format!("{path}.expression"), plan, context)?;
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

fn compile_internal_graph_identity_projection(
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
        "internal graph identity requires exactly one graph variable argument",
        context,
    )?;
    if !plan_uses_variable(plan, &variable) {
        return Err(unsupported(
            format!("{path}.expression.arguments[0]"),
            format!("internal graph identity argument '{variable}' is not a bound graph variable"),
        ));
    }
    Ok(Projection::Expression {
        expression: ScalarExpression::GraphIdentity { variable },
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "graphIdentity".to_string(), variable_name),
    })
}

fn compile_internal_graph_presence_projection(
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
        "internal graph presence requires exactly one graph variable argument",
        context,
    )?;
    if !plan_uses_variable(plan, &variable) {
        return Err(unsupported(
            format!("{path}.expression.arguments[0]"),
            format!("internal graph presence argument '{variable}' is not a bound graph variable"),
        ));
    }
    Ok(Projection::Expression {
        expression: ScalarExpression::GraphPresence { variable },
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "graphPresence".to_string(), variable_name),
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
    let variable = compile_single_graph_value_function_argument(
        function,
        format!("{path}.arguments"),
        "keys() supports exactly one graph variable argument",
        plan,
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
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    compile_scalar_expression_with_plan(expression, path, plan, context)
        .map(OrderExpression::Scalar)
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
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_predicate_scalar_expression(inner, path, plan, context)
        }
        Expression::BinaryOp { .. } => Ok(Some(compile_scalar_expression_in_mode(
            expression, path, plan, context,
        )?)),
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            operand,
            ..
        } if !is_literal_expression(operand) => Ok(Some(compile_scalar_expression_in_mode(
            expression, path, plan, context,
        )?)),
        Expression::Case(case) => Ok(Some(compile_case_scalar_expression_in_mode(
            case,
            path,
            PredicateCompileMode::CaseWhen { plan },
            context,
        )?)),
        Expression::FunctionCall(function)
            if is_id_function(function)
                || is_element_id_function(function)
                || is_type_function(function) =>
        {
            Ok(None)
        }
        Expression::FunctionCall(function) => {
            compile_scalar_function_expression_in_mode(function, path, plan, context)
        }
        _ => Ok(None),
    }
}

fn compile_scalar_predicate_rhs(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarPredicateRhs, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_scalar_predicate_rhs(inner, path, plan, context)
        }
        Expression::BinaryOp { .. }
        | Expression::UnaryOp {
            op: UnaryOperator::Negate,
            ..
        } => Ok(ScalarPredicateRhs::Expression(
            compile_scalar_expression_in_mode(expression, path, plan, context)?,
        )),
        Expression::Case(case) => Ok(ScalarPredicateRhs::Expression(
            compile_case_scalar_expression_in_mode(
                case,
                path,
                PredicateCompileMode::CaseWhen { plan },
                context,
            )?,
        )),
        Expression::FunctionCall(function) => {
            match compile_scalar_function_expression_in_mode(function, path.clone(), plan, context)?
            {
                Some(expression) => Ok(ScalarPredicateRhs::Expression(expression)),
                None => Err(unsupported(
                    path,
                    "scalar predicates support variable.property expressions, scalar literals, scalar parameters, arithmetic expressions, unary negation, nested coalesce(), nullIf(), toString(), toInteger(), toFloat(), toBoolean(), nullable scalar casts, toLower()/lower(), toUpper()/upper(), trim()/btrim(), lTrim(), rTrim(), replace(), size(), char_length(), character_length(), substring(), left(), right(), reverse(), abs(), ceil(), floor(), round(), sqrt(), sign(), exp(), log(), log10(), pi(), e(), sin(), cos(), tan(), cot(), asin(), acos(), atan(), atan2(), degrees(), radians(), or haversin() expressions",
                )),
            }
        }
        Expression::PropertyLookup { .. } => Ok(ScalarPredicateRhs::Expression(
            ScalarExpression::Property(compile_property_ref(expression, path, plan, context)?),
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
                then: compile_scalar_expression_in_mode(
                    &alternative.then,
                    format!("{path}.alternatives[{index}].then"),
                    mode.static_metadata_plan(),
                    context,
                )?,
            })
        })
        .collect::<Result<Vec<_>, CoreError>>()?;
    let else_expression = case
        .default
        .as_ref()
        .map(|expression| {
            compile_scalar_expression_in_mode(
                expression,
                format!("{path}.default"),
                mode.static_metadata_plan(),
                context,
            )
            .map(Box::new)
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
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let variable = compile_single_graph_value_function_argument(
        function,
        format!("{path}.expression.arguments"),
        "keys() supports exactly one graph variable argument",
        plan,
        context,
    )?;
    if !plan_uses_variable(plan, &variable) {
        return Err(unsupported(
            format!("{path}.expression.arguments[0]"),
            format!("keys() argument '{variable}' is not a bound graph variable"),
        ));
    }
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
    let variable = compile_single_graph_value_function_argument(
        function,
        path.clone(),
        message,
        plan,
        context,
    )?;
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

fn compile_single_graph_value_function_argument(
    function: &FunctionInvocation,
    path: impl Into<String>,
    message: &'static str,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    match function.arguments.as_slice() {
        [argument] => compile_graph_value_expression_variable(
            argument,
            format!("{path}[0]"),
            message,
            plan,
            context,
        ),
        [] => {
            let variable = context
                .variable_function_argument(function)
                .map(str::to_string)
                .ok_or_else(|| unsupported(path.clone(), message))?;
            Ok(variable)
        }
        _ => Err(unsupported(path, message)),
    }
}

fn compile_graph_value_expression_variable(
    expression: &Expression,
    path: impl Into<String>,
    message: &'static str,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_graph_value_expression_variable(inner, path, message, plan, context)
        }
        Expression::Variable(variable) => {
            let variable = variable_name(variable);
            Ok(variable)
        }
        Expression::FunctionCall(function)
            if is_start_node_function(function) || is_end_node_function(function) =>
        {
            compile_relationship_endpoint_variable(function, path, plan, context)
        }
        _ => Err(unsupported(path, message)),
    }
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
    plan: &GraphPlan,
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
    reject_unsupported_distinct_aggregate(
        function_kind,
        function.distinct,
        format!("{path}.expression.distinct"),
    )?;
    let target =
        compile_function_aggregate_target(function, function_kind, &path, Some(plan), context)?;
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
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<AggregateTarget, CoreError> {
    match function.arguments.as_slice() {
        [argument] => compile_aggregate_target(
            argument,
            format!("{path}.expression.arguments[0]"),
            plan,
            context,
        ),
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
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<AggregateTarget, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_aggregate_target(inner, path, plan, context),
        Expression::Variable(variable) => Ok(AggregateTarget::VariableKey {
            variable: variable_name(variable),
        }),
        Expression::FunctionCall(function)
            if is_start_node_function(function) || is_end_node_function(function) =>
        {
            let Some(plan) = plan else {
                return Err(unsupported(
                    path,
                    "relationship endpoint aggregate targets require graph context",
                ));
            };
            Ok(AggregateTarget::VariableKey {
                variable: compile_relationship_endpoint_variable(function, path, plan, context)?,
            })
        }
        _ => Ok(AggregateTarget::Property(compile_property_ref(
            expression, path, plan, context,
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
    } else if name.name.eq_ignore_ascii_case("median") {
        Some(AggregateFunction::Median)
    } else if name.name.eq_ignore_ascii_case("stDev") {
        Some(AggregateFunction::StdDev)
    } else if name.name.eq_ignore_ascii_case("stDevP") {
        Some(AggregateFunction::StdDevP)
    } else if name.name.eq_ignore_ascii_case("min") {
        Some(AggregateFunction::Min)
    } else if name.name.eq_ignore_ascii_case("max") {
        Some(AggregateFunction::Max)
    } else {
        None
    }
}

fn reject_unsupported_distinct_aggregate(
    function: AggregateFunction,
    distinct: bool,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    if distinct
        && matches!(
            function,
            AggregateFunction::StdDev | AggregateFunction::StdDevP
        )
    {
        return Err(unsupported(
            path,
            format!(
                "{}(DISTINCT property) is not supported because DataFusion does not execute distinct standard-deviation aggregates",
                aggregate_function_name(function)
            ),
        ));
    }
    Ok(())
}

fn aggregate_function_name(function: AggregateFunction) -> &'static str {
    match function {
        AggregateFunction::Count => "count",
        AggregateFunction::Collect => "collect",
        AggregateFunction::Sum => "sum",
        AggregateFunction::Avg => "avg",
        AggregateFunction::Median => "median",
        AggregateFunction::StdDev => "stDev",
        AggregateFunction::StdDevP => "stDevP",
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

fn is_internal_graph_identity_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name == INTERNAL_GRAPH_IDENTITY_FUNCTION
    )
}

fn is_internal_graph_presence_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name == INTERNAL_GRAPH_PRESENCE_FUNCTION
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

fn is_start_node_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("startNode")
    )
}

fn is_end_node_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("endNode")
    )
}

fn relationship_endpoint_function(function: &FunctionInvocation) -> Option<RelationshipEndpoint> {
    if is_start_node_function(function) {
        Some(RelationshipEndpoint::Start)
    } else if is_end_node_function(function) {
        Some(RelationshipEndpoint::End)
    } else {
        None
    }
}

fn relationship_endpoint_function_name(endpoint: RelationshipEndpoint) -> &'static str {
    match endpoint {
        RelationshipEndpoint::Start => "startNode",
        RelationshipEndpoint::End => "endNode",
    }
}

fn is_length_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("length")
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
        Expression::NodeLabels { base, labels, .. } => match mode.static_metadata_plan() {
            Some(plan) => compile_graph_label_predicate(base, labels, path, plan, context),
            None => Err(unsupported(path, "label predicates require graph context")),
        },
        Expression::Literal(CypherLiteral::Boolean(value)) => {
            Ok(PredicateExpression::Boolean(*value))
        }
        Expression::IsNull {
            operand, negated, ..
        } => compile_null_predicate(operand, *negated, path, mode, context),
        Expression::FunctionCall(function) if is_exists_function(function) => {
            Ok(PredicateExpression::Comparison(compile_exists_predicate(
                function,
                path,
                mode.graph_plan(),
                context,
            )?))
        }
        Expression::FunctionCall(function) if is_empty_function(function) => {
            Ok(PredicateExpression::ScalarComparison(
                compile_is_empty_predicate(function, path, mode.static_metadata_plan(), context)?,
            ))
        }
        Expression::PropertyLookup { .. } => {
            Ok(PredicateExpression::Comparison(PropertyPredicate {
                property: compile_property_ref(expression, path, mode.graph_plan(), context)?,
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

fn compile_is_empty_predicate(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
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
            expression: Box::new(compile_scalar_expression_in_mode(
                argument,
                format!("{path}.arguments[0]"),
                plan,
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
    if let Some(property) =
        compile_optional_property_ref(lhs, format!("{path}.lhs"), mode.graph_plan(), context)?
    {
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
    if let Some(predicate) =
        compile_optional_scalar_binary_comparison(lhs, operator, rhs, &path, mode, context)?
    {
        return Ok(predicate);
    }
    if let Some(property) =
        compile_optional_property_ref(rhs, format!("{path}.rhs"), mode.graph_plan(), context)?
    {
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

fn compile_optional_scalar_binary_comparison(
    lhs: &Expression,
    operator: ComparisonOperator,
    rhs: &Expression,
    path: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    if let Some(lhs) = compile_optional_predicate_scalar_expression(
        lhs,
        format!("{path}.lhs"),
        mode.static_metadata_plan(),
        context,
    )? {
        return Ok(Some(PredicateExpression::ScalarComparison(
            ScalarPredicate {
                lhs,
                operator,
                rhs: compile_scalar_predicate_rhs(
                    rhs,
                    format!("{path}.rhs"),
                    mode.static_metadata_plan(),
                    context,
                )?,
            },
        )));
    }
    let Some(rhs) = compile_optional_predicate_scalar_expression(
        rhs,
        format!("{path}.rhs"),
        mode.static_metadata_plan(),
        context,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(PredicateExpression::ScalarComparison(
        ScalarPredicate {
            lhs: rhs,
            operator: invert_comparison_operator(operator, format!("{path}.operator"))?,
            rhs: compile_scalar_predicate_rhs(
                lhs,
                format!("{path}.lhs"),
                mode.static_metadata_plan(),
                context,
            )?,
        },
    )))
}

fn compile_left_property_comparison(
    property: PropertyRef,
    operator: ComparisonOperator,
    rhs: &Expression,
    path: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    if let Some(predicate) = compile_dynamic_string_property_predicate(
        &property,
        operator,
        rhs,
        path,
        mode.static_metadata_plan(),
        context,
    )? {
        return Ok(predicate);
    }
    if let Some(predicate) = compile_dynamic_scalar_property_predicate(
        &property,
        operator,
        rhs,
        path,
        mode.static_metadata_plan(),
        context,
    )? {
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
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    if !is_string_comparison_operator(operator) || is_literal_expression(rhs) {
        return Ok(None);
    }

    let Some(rhs) =
        compile_optional_predicate_scalar_expression(rhs, format!("{path}.rhs"), plan, context)?
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
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    if is_string_comparison_operator(operator) || is_literal_expression(rhs) {
        return Ok(None);
    }

    let Some(rhs) =
        compile_optional_predicate_scalar_expression(rhs, format!("{path}.rhs"), plan, context)?
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
    if let Some(property) =
        compile_optional_property_ref(lhs, format!("{path}.lhs"), mode.graph_plan(), context)?
    {
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
    if let Some(lhs) = compile_optional_predicate_scalar_expression(
        lhs,
        format!("{path}.lhs"),
        mode.static_metadata_plan(),
        context,
    )? {
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
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let variable = compile_graph_value_expression_variable(
        base,
        format!("{path}.base"),
        "graph label predicates require a node or relationship variable",
        plan,
        context,
    )?;
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
            let variable = compile_single_graph_value_function_argument(
                function,
                format!("{path}.arguments"),
                "keys() supports exactly one graph variable argument",
                plan,
                context,
            )?;
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
            expression,
            path,
            mode.graph_plan(),
            context,
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
    if let Some(property) = compile_optional_property_ref(
        operand,
        format!("{path}.operand"),
        mode.graph_plan(),
        context,
    )? {
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
    if let Some(lhs) = compile_optional_predicate_scalar_expression(
        operand,
        format!("{path}.operand"),
        mode.static_metadata_plan(),
        context,
    )? {
        return Ok(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs,
            operator,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null)),
        }));
    }
    if let Some(plan) = mode.graph_plan() {
        if let Some(variable) =
            compile_optional_graph_variable_ref(operand, format!("{path}.operand"), plan, context)?
        {
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

fn compile_optional_graph_variable_ref(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<String>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_graph_variable_ref(inner, path, plan, context)
        }
        Expression::Variable(variable) => Ok(Some(variable_name(variable))),
        Expression::FunctionCall(function)
            if is_start_node_function(function) || is_end_node_function(function) =>
        {
            Ok(Some(compile_relationship_endpoint_variable(
                function, path, plan, context,
            )?))
        }
        _ => Ok(None),
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
    let variable = compile_single_graph_value_function_argument(
        function,
        format!("{path}.arguments"),
        "id() supports exactly one graph variable argument",
        plan,
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
    let variable = compile_single_graph_value_function_argument(
        function,
        format!("{path}.arguments"),
        "elementId() supports exactly one graph variable argument",
        plan,
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
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<PropertyRef, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_property_ref(inner, path, plan, context),
        Expression::PropertyLookup { base, property, .. } => Ok(PropertyRef {
            variable: compile_property_base_variable(base, format!("{path}.base"), plan, context)?,
            property: property.name.name.clone(),
        }),
        _ => Err(unsupported(
            path,
            "only variable.property expressions are supported here",
        )),
    }
}

fn compile_optional_property_ref(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<PropertyRef>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_property_ref(inner, path, plan, context)
        }
        Expression::PropertyLookup { .. } => {
            compile_property_ref(expression, path, plan, context).map(Some)
        }
        _ => Ok(None),
    }
}

fn compile_property_base_variable(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_property_base_variable(inner, path, plan, context)
        }
        Expression::Variable(variable) => Ok(variable_name(variable)),
        Expression::FunctionCall(function)
            if is_start_node_function(function) || is_end_node_function(function) =>
        {
            let Some(plan) = plan else {
                return Err(unsupported(
                    path,
                    "relationship endpoint property references require graph context",
                ));
            };
            compile_relationship_endpoint_variable(function, path, plan, context)
        }
        _ => Err(unsupported(
            path,
            "property references must be variable.property or startNode()/endNode() relationship endpoint properties",
        )),
    }
}

fn compile_relationship_endpoint_variable(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    let endpoint = relationship_endpoint_function(function).ok_or_else(|| {
        unsupported(
            path.clone(),
            format!(
                "function '{}' is not a relationship endpoint function",
                qualified_function_name(function)
            ),
        )
    })?;
    let function_name = relationship_endpoint_function_name(endpoint);
    let variable = compile_single_variable_function_argument(
        function,
        format!("{path}.arguments"),
        match endpoint {
            RelationshipEndpoint::Start => {
                "startNode() supports exactly one relationship variable argument"
            }
            RelationshipEndpoint::End => {
                "endNode() supports exactly one relationship variable argument"
            }
        },
        context,
    )?;
    let (relationship_index, relationship) = plan
        .relationships
        .iter()
        .enumerate()
        .find(|(_, relationship)| relationship.variable.as_deref() == Some(variable.as_str()))
        .ok_or_else(|| {
            unsupported(
                format!("{path}.arguments[0]"),
                format!(
                    "{function_name}() argument '{variable}' is not a named relationship variable"
                ),
            )
        })?;
    if plan.optional_relationships.contains(&relationship_index) {
        return Err(unsupported(
            path,
            format!(
                "{function_name}() over optional relationship variables is not supported yet because missing relationships require nullable endpoint expressions"
            ),
        ));
    }
    match relationship.direction {
        Direction::Outgoing => Ok(match endpoint {
            RelationshipEndpoint::Start => relationship.left.clone(),
            RelationshipEndpoint::End => relationship.right.clone(),
        }),
        Direction::Incoming => Ok(match endpoint {
            RelationshipEndpoint::Start => relationship.right.clone(),
            RelationshipEndpoint::End => relationship.left.clone(),
        }),
        Direction::Undirected => Err(unsupported(
            path,
            format!(
                "{function_name}() over undirected relationships is not supported yet because endpoint orientation is data-dependent"
            ),
        )),
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
    fresh_internal_node_variable_avoiding(plan, part_index, node_index, "")
}

fn fresh_internal_node_variable_avoiding(
    plan: &GraphPlan,
    part_index: usize,
    node_index: usize,
    avoid: &str,
) -> String {
    let mut suffix = 0;
    loop {
        let candidate = if suffix == 0 {
            format!("__coral_node_{part_index}_{node_index}")
        } else {
            format!("__coral_node_{part_index}_{node_index}_{suffix}")
        };
        if candidate != avoid && !plan_uses_variable(plan, &candidate) {
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

fn fresh_hidden_graph_variable(
    plan: &GraphPlan,
    state: &CypherCompileState,
    variable: &str,
) -> String {
    let mut suffix = 0;
    loop {
        let candidate = if suffix == 0 {
            format!("__coral_hidden_{variable}")
        } else {
            format!("__coral_hidden_{variable}_{suffix}")
        };
        if !plan_uses_variable(plan, &candidate)
            && !state.hidden_graph_variables.contains(&candidate)
        {
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
    if labels.is_empty() {
        return Err(unsupported(
            path,
            "exactly one positive static label or relationship type is required",
        ));
    }

    let mut required = BTreeSet::new();
    let mut forbidden = BTreeSet::new();
    for (index, label) in labels.iter().enumerate() {
        collect_static_label_requirements(
            label,
            &mut required,
            &mut forbidden,
            format!("{path}[{index}]"),
        )?;
    }

    let mut required_labels = required.iter();
    let Some(label) = required_labels.next() else {
        return Err(unsupported(
            path,
            "node and relationship patterns require exactly one positive static label or relationship type",
        ));
    };
    if required_labels.next().is_some() {
        return Err(unsupported(
            path,
            "node and relationship patterns require exactly one positive static label or relationship type",
        ));
    }
    if forbidden.contains(label) {
        return Err(unsupported(
            path,
            "contradictory label expressions cannot be represented by one Coral mapping",
        ));
    }
    Ok((*label).clone())
}

fn collect_static_label_requirements(
    expression: &LabelExpression,
    required: &mut BTreeSet<String>,
    forbidden: &mut BTreeSet<String>,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    match expression {
        LabelExpression::Static(name) => {
            required.insert(name.name.clone());
            Ok(())
        }
        LabelExpression::Dynamic { .. } => Err(unsupported(
            path,
            "dynamic label expressions are not supported yet",
        )),
        LabelExpression::Or { .. } => Err(unsupported(
            path,
            "label/type alternatives require union planning and are not supported yet",
        )),
        LabelExpression::And { lhs, rhs, .. } => {
            collect_static_label_requirements(lhs, required, forbidden, format!("{path}.lhs"))?;
            collect_static_label_requirements(rhs, required, forbidden, format!("{path}.rhs"))
        }
        LabelExpression::Not { inner, .. } => {
            collect_static_label_exclusion(inner, forbidden, format!("{path}.inner"))
        }
        LabelExpression::Group { inner, .. } => {
            collect_static_label_requirements(inner, required, forbidden, path)
        }
    }
}

fn collect_static_label_exclusion(
    expression: &LabelExpression,
    forbidden: &mut BTreeSet<String>,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    match expression {
        LabelExpression::Static(name) => {
            forbidden.insert(name.name.clone());
            Ok(())
        }
        LabelExpression::Group { inner, .. } => {
            collect_static_label_exclusion(inner, forbidden, path)
        }
        LabelExpression::Dynamic { .. } => Err(unsupported(
            path,
            "dynamic label expressions are not supported yet",
        )),
        LabelExpression::And { .. } | LabelExpression::Or { .. } | LabelExpression::Not { .. } => {
            Err(unsupported(
                path,
                "negated compound label expressions are not supported yet",
            ))
        }
    }
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
                nulls: None,
            }]
        );
        assert_eq!(plan.limit, Some(10));
        assert_eq!(plan.predicate, None);
    }

    #[test]
    fn compiles_union_query() {
        let query = compile_cypher_query(
            "MATCH (service:Service) \
             WHERE service.tier = 'prod' \
             RETURN service.name AS item \
             UNION \
             MATCH (person:Person) \
             WHERE person.team = 'platform' \
             RETURN person.name AS item",
        )
        .expect("UNION query should compile");

        let GraphQuery::Union(union) = query else {
            panic!("expected union query");
        };
        assert_eq!(projection_names(&union.first), vec!["item".to_string()]);
        assert_eq!(union.branches.len(), 1);
        let branch = union.branches.first().expect("union branch should exist");
        assert!(!branch.all);
        assert_eq!(projection_names(&branch.plan), vec!["item".to_string()]);
    }

    #[test]
    fn compiles_union_all_query() {
        let query = compile_cypher_query(
            "MATCH (service:Service) RETURN service.tier AS tier \
             UNION ALL \
             MATCH (service:Service) RETURN service.tier AS tier",
        )
        .expect("UNION ALL query should compile");

        let GraphQuery::Union(union) = query else {
            panic!("expected union query");
        };
        assert_eq!(union.branches.len(), 1);
        let branch = union.branches.first().expect("union branch should exist");
        assert!(branch.all);
    }

    #[test]
    fn compiles_static_node_label_alternatives_as_union_all() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team) \
             RETURN entity.name AS name",
        )
        .expect("static node label alternatives should compile");

        let GraphQuery::Union(union) = query else {
            panic!("expected static label alternatives to expand into a union query");
        };
        assert_eq!(
            union.first.nodes.first().expect("first node").label,
            "Person"
        );
        assert_eq!(union.branches.len(), 1);
        let branch = union.branches.first().expect("alternative branch");
        assert!(branch.all);
        assert_eq!(
            branch.plan.nodes.first().expect("branch node").label,
            "Team"
        );
        assert_eq!(projection_names(&union.first), vec!["name".to_string()]);
        assert_eq!(projection_names(&branch.plan), vec!["name".to_string()]);
    }

    #[test]
    fn deduplicates_static_node_label_alternatives_before_union_expansion() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Person) \
             RETURN entity.name AS name",
        )
        .expect("duplicate static node label alternatives should compile");

        let GraphQuery::Plan(plan) = query else {
            panic!("duplicate static label alternatives should collapse to one graph plan");
        };
        assert_eq!(plan.nodes.first().expect("first node").label, "Person");
    }

    #[test]
    fn deduplicates_static_relationship_type_alternatives_before_union_expansion() {
        let query = compile_cypher_query(
            "MATCH (source:Service)-[relationship:DEPENDS_ON|DEPENDS_ON]->(target:Service) \
             RETURN type(relationship) AS relationship_type",
        )
        .expect("duplicate static relationship type alternatives should compile");

        let GraphQuery::Plan(plan) = query else {
            panic!(
                "duplicate static relationship type alternatives should collapse to one graph plan"
            );
        };
        assert_eq!(
            plan.relationships
                .first()
                .expect("first relationship")
                .relationship_type,
            "DEPENDS_ON"
        );
    }

    #[test]
    fn rejects_static_label_alternatives_that_exceed_branch_cap() {
        let labels = (0..=MAX_PATTERN_ALTERNATIVE_BRANCHES)
            .map(|index| format!("Label{index}"))
            .collect::<Vec<_>>()
            .join("|");
        let cypher = format!("MATCH (entity:{labels}) RETURN entity.name AS name");

        let error = compile_cypher_query(&cypher)
            .expect_err("excessive static label alternatives should be capped");

        assert!(error.to_string().contains("more than 64 branches"));
    }

    #[test]
    fn compiles_static_relationship_type_alternatives_as_union_all() {
        let query = compile_cypher_query(
            "MATCH (source:Service)-[relationship:DEPENDS_ON|OWNS]->(target:Service) \
             RETURN type(relationship) AS relationship_type",
        )
        .expect("static relationship type alternatives should compile");

        let GraphQuery::Union(union) = query else {
            panic!("expected static relationship type alternatives to expand into a union query");
        };
        assert_eq!(
            union
                .first
                .relationships
                .first()
                .expect("first relationship")
                .relationship_type,
            "DEPENDS_ON"
        );
        assert_eq!(union.branches.len(), 1);
        let branch = union.branches.first().expect("alternative branch");
        assert!(branch.all);
        assert_eq!(
            branch
                .plan
                .relationships
                .first()
                .expect("branch relationship")
                .relationship_type,
            "OWNS"
        );
    }

    #[test]
    fn compiles_static_label_alternatives_with_outer_count_star() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team) \
             RETURN count(*) AS count",
        )
        .expect("count(*) should compile as an outer union aggregate");

        let GraphQuery::Union(union) = query else {
            panic!("expected static label alternatives to expand into a union query");
        };
        assert_eq!(
            union.outer_projection,
            Some(GraphUnionOuterProjection {
                items: vec![GraphUnionOuterProjectionItem::CountAll {
                    alias: "count".to_string(),
                }],
                group_by: Vec::new(),
            })
        );
        assert_eq!(
            union.first.projection_output_names(),
            vec!["__coral_count_row".to_string()]
        );
    }

    #[test]
    fn compiles_static_label_alternatives_with_outer_count_star_ordering() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team) \
             RETURN count(*) AS count \
             ORDER BY count(*)",
        )
        .expect("count(*) order expression should compile as an outer union aggregate alias");

        let GraphQuery::Union(union) = query else {
            panic!("expected static label alternatives to expand into a union query");
        };
        assert_eq!(
            union.order_by,
            vec![OrderKey {
                expression: OrderExpression::ProjectionAlias("count".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            }]
        );
    }

    #[test]
    fn compiles_static_label_alternatives_with_grouped_count_star() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team) \
             RETURN entity.name AS name, count(*) AS count",
        )
        .expect("grouped count(*) should compile as an outer union aggregate");

        let GraphQuery::Union(union) = query else {
            panic!("expected static label alternatives to expand into a union query");
        };
        assert_eq!(
            union.outer_projection,
            Some(GraphUnionOuterProjection {
                items: vec![
                    GraphUnionOuterProjectionItem::Column {
                        name: "name".to_string(),
                    },
                    GraphUnionOuterProjectionItem::CountAll {
                        alias: "count".to_string(),
                    },
                ],
                group_by: vec!["name".to_string()],
            })
        );
        assert_eq!(
            union.first.projection_output_names(),
            vec!["name".to_string()]
        );
    }

    #[test]
    fn compiles_static_label_alternatives_with_grouped_count_star_ordering() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team) \
             RETURN entity.name AS name, count(*) AS count \
             ORDER BY count(*) DESC, entity.name",
        )
        .expect("grouped count(*) order expressions should compile");

        let GraphQuery::Union(union) = query else {
            panic!("expected static label alternatives to expand into a union query");
        };
        assert_eq!(
            union.order_by,
            vec![
                OrderKey {
                    expression: OrderExpression::ProjectionAlias("count".to_string()),
                    direction: OrderDirection::Descending,
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::ProjectionAlias("name".to_string()),
                    direction: OrderDirection::Ascending,
                    nulls: None,
                },
            ]
        );
    }

    #[test]
    fn compiles_static_label_alternatives_with_grouped_count_star_first() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team) \
             RETURN count(*) AS count, entity.name AS name",
        )
        .expect("grouped count(*) should preserve RETURN item order");

        let GraphQuery::Union(union) = query else {
            panic!("expected static label alternatives to expand into a union query");
        };
        let outer_projection = union
            .outer_projection
            .expect("expected an outer union projection");
        assert_eq!(
            outer_projection.output_names(),
            vec!["count".to_string(), "name".to_string()]
        );
        assert_eq!(outer_projection.group_by, vec!["name".to_string()]);
    }

    #[test]
    fn compiles_static_label_alternatives_with_grouped_count_property() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, count(service.name) AS named_services \
             ORDER BY count(service.name) DESC, name",
        )
        .expect("grouped count(property) should compile as an outer union aggregate");

        let GraphQuery::Union(union) = query else {
            panic!("expected static label alternatives to expand into a union query");
        };
        assert_eq!(
            union.first.projection_output_names(),
            vec!["name".to_string(), "__coral_agg_1".to_string()]
        );
        assert_eq!(
            union.outer_projection,
            Some(GraphUnionOuterProjection {
                items: vec![
                    GraphUnionOuterProjectionItem::Column {
                        name: "name".to_string(),
                    },
                    GraphUnionOuterProjectionItem::Aggregate {
                        function: AggregateFunction::Count,
                        source: "__coral_agg_1".to_string(),
                        distinct: false,
                        alias: "named_services".to_string(),
                    },
                ],
                group_by: vec!["name".to_string()],
            })
        );
        assert_eq!(
            union.order_by,
            vec![
                OrderKey {
                    expression: OrderExpression::ProjectionAlias("named_services".to_string()),
                    direction: OrderDirection::Descending,
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::ProjectionAlias("name".to_string()),
                    direction: OrderDirection::Ascending,
                    nulls: None,
                },
            ]
        );
    }

    #[test]
    fn compiles_static_label_alternatives_with_count_node_projection() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, count(service) AS services \
             ORDER BY count(service) DESC, name",
        )
        .expect("count(node) should compile as an outer union aggregate");

        let GraphQuery::Union(union) = query else {
            panic!("expected static label alternatives to expand into a union query");
        };
        assert_eq!(
            union.first.projection_output_names(),
            vec!["name".to_string(), "__coral_agg_1".to_string()]
        );
        assert!(matches!(
            union.first.projections.get(1),
            Some(Projection::Expression {
                expression: ScalarExpression::GraphPresence { variable },
                alias,
            }) if variable == "service" && alias == "__coral_agg_1"
        ));
        assert_eq!(
            union.outer_projection,
            Some(GraphUnionOuterProjection {
                items: vec![
                    GraphUnionOuterProjectionItem::Column {
                        name: "name".to_string(),
                    },
                    GraphUnionOuterProjectionItem::Aggregate {
                        function: AggregateFunction::Count,
                        source: "__coral_agg_1".to_string(),
                        distinct: false,
                        alias: "services".to_string(),
                    },
                ],
                group_by: vec!["name".to_string()],
            })
        );
        assert_eq!(
            union.order_by,
            vec![
                OrderKey {
                    expression: OrderExpression::ProjectionAlias("services".to_string()),
                    direction: OrderDirection::Descending,
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::ProjectionAlias("name".to_string()),
                    direction: OrderDirection::Ascending,
                    nulls: None,
                },
            ]
        );
    }

    #[test]
    fn compiles_static_label_alternatives_with_distinct_count_node_projection() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN count(DISTINCT entity) AS owners",
        )
        .expect("distinct graph variable counts should compile through graph identity");

        let GraphQuery::Union(union) = query else {
            panic!("expected static label alternatives to expand into a union query");
        };
        assert_eq!(
            union.first.projections,
            vec![Projection::Expression {
                expression: ScalarExpression::GraphIdentity {
                    variable: "entity".to_string(),
                },
                alias: "__coral_agg_0".to_string(),
            }]
        );
        assert_eq!(
            union.outer_projection,
            Some(GraphUnionOuterProjection {
                items: vec![GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Count,
                    source: "__coral_agg_0".to_string(),
                    distinct: true,
                    alias: "owners".to_string(),
                }],
                group_by: Vec::new(),
            })
        );
    }

    #[test]
    fn compiles_static_label_alternatives_with_distinct_count_property() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN count(DISTINCT service.name) AS named_services",
        )
        .expect("count(DISTINCT property) should compile as an outer union aggregate");

        let GraphQuery::Union(union) = query else {
            panic!("expected static label alternatives to expand into a union query");
        };
        let outer_projection = union
            .outer_projection
            .expect("expected an outer union projection");
        assert_eq!(
            outer_projection.items,
            vec![GraphUnionOuterProjectionItem::Aggregate {
                function: AggregateFunction::Count,
                source: "__coral_agg_0".to_string(),
                distinct: true,
                alias: "named_services".to_string(),
            }]
        );
    }

    #[test]
    fn compiles_static_label_alternatives_with_numeric_property_aggregates() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, \
                    sum(service.risk) AS total_risk, \
                    avg(service.risk) AS average_risk, \
                    min(service.risk) AS lowest_risk, \
                    max(service.risk) AS highest_risk \
             ORDER BY sum(service.risk) DESC",
        )
        .expect("numeric property aggregates should compile as outer union aggregates");

        let GraphQuery::Union(union) = query else {
            panic!("expected static label alternatives to expand into a union query");
        };
        let outer_projection = union
            .outer_projection
            .expect("expected an outer union projection");
        assert_eq!(
            outer_projection.output_names(),
            vec![
                "name".to_string(),
                "total_risk".to_string(),
                "average_risk".to_string(),
                "lowest_risk".to_string(),
                "highest_risk".to_string(),
            ]
        );
        assert_eq!(
            union.first.projection_output_names(),
            vec![
                "name".to_string(),
                "__coral_agg_1".to_string(),
                "__coral_agg_2".to_string(),
                "__coral_agg_3".to_string(),
                "__coral_agg_4".to_string(),
            ]
        );
        assert_eq!(
            union.order_by,
            vec![OrderKey {
                expression: OrderExpression::ProjectionAlias("total_risk".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            }]
        );
    }

    #[test]
    fn compiles_static_label_alternatives_with_collect_property_projection() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, collect(DISTINCT service.name) AS services \
             ORDER BY name",
        )
        .expect("collect(property) should compile as an outer union aggregate");

        let GraphQuery::Union(union) = query else {
            panic!("expected static label alternatives to expand into a union query");
        };
        assert_eq!(
            union.outer_projection,
            Some(GraphUnionOuterProjection {
                items: vec![
                    GraphUnionOuterProjectionItem::Column {
                        name: "name".to_string(),
                    },
                    GraphUnionOuterProjectionItem::Aggregate {
                        function: AggregateFunction::Collect,
                        source: "__coral_agg_1".to_string(),
                        distinct: true,
                        alias: "services".to_string(),
                    },
                ],
                group_by: vec!["name".to_string()],
            })
        );
    }

    #[test]
    fn compiles_static_label_alternatives_with_outer_row_modifiers() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team) \
             RETURN entity.name AS name \
             ORDER BY name DESC \
             SKIP 1 \
             LIMIT 5",
        )
        .expect("global row modifiers should compile as outer union modifiers");

        let GraphQuery::Union(union) = query else {
            panic!("expected static label alternatives to expand into a union query");
        };
        assert!(union.first.order_by.is_empty());
        assert_eq!(union.first.skip, None);
        assert_eq!(union.first.limit, None);
        assert_eq!(
            union.order_by,
            vec![OrderKey {
                expression: OrderExpression::ProjectionAlias("name".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            }]
        );
        assert_eq!(union.skip, Some(1));
        assert_eq!(union.limit, Some(5));
    }

    #[test]
    fn compiles_static_label_alternatives_with_outer_distinct() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team) \
             RETURN DISTINCT entity.name AS name \
             ORDER BY name",
        )
        .expect("RETURN DISTINCT should compile as an outer union modifier");

        let GraphQuery::Union(union) = query else {
            panic!("expected static label alternatives to expand into a union query");
        };
        assert!(!union.first.distinct);
        assert!(union.distinct);
        assert_eq!(
            union.order_by,
            vec![OrderKey {
                expression: OrderExpression::ProjectionAlias("name".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            }]
        );
    }

    #[test]
    fn compiles_static_label_alternatives_with_projected_global_ordering() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team) \
             RETURN entity.name AS name \
             ORDER BY entity.name",
        )
        .expect("projected global ordering should compile");

        let GraphQuery::Union(union) = query else {
            panic!("expected static label alternatives to expand into a union query");
        };
        assert_eq!(
            union.order_by,
            vec![OrderKey {
                expression: OrderExpression::ProjectionAlias("name".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            }]
        );
    }

    #[test]
    fn compiles_static_label_alternatives_with_hidden_global_ordering() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team) \
             RETURN entity.name AS name \
             ORDER BY entity.team",
        )
        .expect("row-preserving hidden global ordering should compile");

        let GraphQuery::Union(union) = query else {
            panic!("expected static label alternatives to expand into a union query");
        };
        assert_eq!(
            union.first.projection_output_names(),
            vec!["name".to_string(), "__coral_order_0".to_string()]
        );
        assert_eq!(
            union.outer_projection,
            Some(GraphUnionOuterProjection {
                items: vec![GraphUnionOuterProjectionItem::Column {
                    name: "name".to_string(),
                }],
                group_by: Vec::new(),
            })
        );
        assert_eq!(
            union.order_by,
            vec![OrderKey {
                expression: OrderExpression::ProjectionAlias("__coral_order_0".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            }]
        );
    }

    #[test]
    fn rejects_static_label_alternatives_with_aggregate_hidden_global_ordering() {
        let error = compile_cypher_query(
            "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, count(*) AS services \
             ORDER BY service.name",
        )
        .expect_err("aggregate hidden global ordering should require staged planning");

        assert!(error.to_string().contains("aggregate RETURN"));
    }

    #[test]
    fn compiles_static_label_alternatives_with_terminal_with_projection() {
        let query = compile_cypher_query(
            "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
             WITH owner.name AS owner, service.name AS service \
             WHERE service = 'billing-api' \
             RETURN owner, service \
             ORDER BY owner",
        )
        .expect("static alternatives with terminal WITH projection should compile");

        let GraphQuery::Union(union) = query else {
            panic!("expected static label alternatives to expand into a union query");
        };
        assert_eq!(
            union.first.projection_output_names(),
            vec!["owner".to_string(), "service".to_string()]
        );
        assert!(union.first.post_projection_predicate.is_some());
        assert!(union.branches.iter().all(|branch| {
            branch.plan.projection_output_names()
                == vec!["owner".to_string(), "service".to_string()]
                && branch.plan.post_projection_predicate.is_some()
        }));
        assert_eq!(
            union.order_by,
            vec![OrderKey {
                expression: OrderExpression::ProjectionAlias("owner".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            }]
        );
    }

    #[test]
    fn compiles_static_label_alternatives_inside_explicit_union_all() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team) RETURN entity.name AS item \
             UNION ALL \
             MATCH (service:Service) RETURN service.name AS item",
        )
        .expect("static alternatives should flatten into top-level UNION ALL");

        let GraphQuery::Union(union) = query else {
            panic!("expected union query");
        };
        assert_eq!(projection_names(&union.first), vec!["item".to_string()]);
        assert_eq!(union.branches.len(), 2);
        assert!(union.branches.iter().all(|branch| branch.all));
        assert_eq!(
            union
                .branches
                .iter()
                .map(|branch| projection_names(&branch.plan))
                .collect::<Vec<_>>(),
            vec![vec!["item".to_string()], vec!["item".to_string()]]
        );
    }

    #[test]
    fn compiles_static_label_alternatives_inside_explicit_union_distinct() {
        let query = compile_cypher_query(
            "MATCH (entity:Person|Team) RETURN entity.name AS item \
             UNION \
             MATCH (service:Service) RETURN service.name AS item",
        )
        .expect("static alternatives should flatten into top-level UNION distinct");

        let GraphQuery::Union(union) = query else {
            panic!("expected union query");
        };
        assert!(union.distinct);
        assert_eq!(projection_names(&union.first), vec!["item".to_string()]);
        assert_eq!(union.branches.len(), 2);
        assert!(union.branches.iter().all(|branch| branch.all));
        assert_eq!(
            union
                .branches
                .iter()
                .map(|branch| branch
                    .plan
                    .nodes
                    .first()
                    .expect("branch node")
                    .label
                    .as_str())
                .collect::<Vec<_>>(),
            vec!["Team", "Service"]
        );
    }

    #[test]
    fn rejects_static_label_alternatives_inside_mixed_explicit_union() {
        let error = compile_cypher_query(
            "MATCH (entity:Person|Team) RETURN entity.name AS item \
             UNION \
             MATCH (service:Service) RETURN service.name AS item \
             UNION ALL \
             MATCH (person:Person) RETURN person.name AS item",
        )
        .expect_err("mixed UNION operators need nested grouping for static alternatives");

        assert!(error.to_string().contains("mixed UNION and UNION ALL"));
    }

    #[test]
    fn rejects_static_label_alternatives_with_modifiers_inside_explicit_union_all() {
        let error = compile_cypher_query(
            "MATCH (entity:Person|Team) \
             RETURN entity.name AS item \
             ORDER BY item \
             UNION ALL \
             MATCH (service:Service) RETURN service.name AS item",
        )
        .expect_err("branch-level modifiers need nested grouping");

        assert!(error.to_string().contains("nested union grouping"));
    }

    #[test]
    fn rejects_union_projection_mismatches() {
        let error = compile_cypher_query(
            "MATCH (service:Service) RETURN service.name AS item \
             UNION \
             MATCH (person:Person) RETURN person.name AS person",
        )
        .expect_err("mismatched UNION projections should fail");

        assert!(error.to_string().contains("UNION branch projections"));
    }

    #[test]
    fn single_plan_compile_rejects_union_queries() {
        let error = compile_cypher(
            "MATCH (service:Service) RETURN service.name AS item \
             UNION \
             MATCH (person:Person) RETURN person.name AS item",
        )
        .expect_err("single-plan compiler should reject UNION");

        assert!(error.to_string().contains("compile_cypher"));
    }

    #[test]
    fn compiles_ignored_path_variable_patterns() {
        let plan = compile_cypher(
            "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             RETURN person.name AS owner, service.name AS service",
        )
        .expect("non-materialized path binding should compile");

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
    }

    #[test]
    fn compiles_path_length_projection() {
        let plan = compile_cypher(
            "MATCH path = (source:Service)-[:DEPENDS_ON*2]->(target:Service) \
             RETURN source.name AS source, target.name AS target, length(path) AS hops",
        )
        .expect("path length projection should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "source".to_string(),
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
                Projection::Expression {
                    expression: ScalarExpression::Literal(Literal::Integer(2)),
                    alias: "hops".to_string(),
                },
            ]
        );
    }

    #[test]
    fn compiles_terminal_with_path_length_projection() {
        let plan = compile_cypher(
            "MATCH path = (source:Service)-[:DEPENDS_ON]->{2}(target:Service) \
             WITH source.name AS source, target.name AS target, length(path) AS hops \
             RETURN source, target, hops",
        )
        .expect("terminal WITH path length projection should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "source".to_string(),
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
                Projection::Expression {
                    expression: ScalarExpression::Literal(Literal::Integer(2)),
                    alias: "hops".to_string(),
                },
            ]
        );
    }

    #[test]
    fn rejects_length_over_non_path_variable() {
        let error = compile_cypher(
            "MATCH (service:Service) \
             RETURN length(service) AS length",
        )
        .expect_err("length() should only accept bound path variables");

        assert!(
            error
                .to_string()
                .contains("length() argument 'service' is not a bound path variable"),
            "{error}"
        );
    }

    #[test]
    fn rejects_path_variable_collisions() {
        let error = compile_cypher(
            "MATCH path = (path:Person)-[:OWNS]->(service:Service) \
             RETURN service.name AS service",
        )
        .expect_err("path bindings must not collide with graph variables");

        assert!(
            error.to_string().contains("path variable 'path' conflicts"),
            "{error}"
        );
    }

    #[test]
    fn rejects_graph_variables_that_shadow_in_scope_path_variables() {
        let error = compile_cypher(
            "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             MATCH (path:Person) \
             RETURN path.name AS person",
        )
        .expect_err("graph variables must not shadow in-scope path variables");

        assert!(
            error
                .to_string()
                .contains("graph variable 'path' conflicts with an in-scope path variable"),
            "{error}"
        );
    }

    #[test]
    fn explicit_with_drops_path_variables() {
        let plan = compile_cypher(
            "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH person, service \
             MATCH (path:Person) \
             RETURN path.name AS person",
        )
        .expect("explicit WITH should drop unsupported path values");

        assert!(plan.nodes.iter().any(|node| node.variable == "path"));
    }

    #[test]
    fn rejects_with_star_over_path_variables() {
        let error = compile_cypher(
            "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH * \
             RETURN person.name AS owner",
        )
        .expect_err("WITH * must not implicitly carry unsupported path values");

        assert!(
            error
                .to_string()
                .contains("WITH * cannot carry path variables"),
            "{error}"
        );
    }

    #[test]
    fn rejects_path_value_property_projections() {
        let error = compile_cypher(
            "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             RETURN path.name AS path_name",
        )
        .expect_err("path values should not be projected as graph properties");

        assert_path_value_error(&error);
    }

    #[test]
    fn rejects_path_value_property_predicates() {
        let error = compile_cypher(
            "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WHERE path.name = 'x' \
             RETURN person.name AS owner",
        )
        .expect_err("path values should not be filtered as graph properties");

        assert_path_value_error(&error);
    }

    #[test]
    fn rejects_path_value_property_ordering() {
        let error = compile_cypher(
            "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             RETURN person.name AS owner \
             ORDER BY path.name",
        )
        .expect_err("path values should not be ordered as graph properties");

        assert_path_value_error(&error);
    }

    #[test]
    fn rejects_transparent_with_path_value_predicates_before_dropping_path_values() {
        let error = compile_cypher(
            "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH person, service WHERE path.name = 'x' \
             RETURN person.name AS owner",
        )
        .expect_err("transparent WITH should reject path values before dropping them");

        assert_path_value_error(&error);
    }

    fn assert_path_value_error(error: &CoreError) {
        assert!(
            error
                .to_string()
                .contains("path variable 'path' cannot be used as a graph value"),
            "{error}"
        );
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
    fn compiles_transparent_with_dropped_variables() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WITH service \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS source, target.name AS target",
        )
        .expect("transparent WITH should allow dropping graph variables");

        assert_eq!(plan.nodes.len(), 3);
        assert!(
            plan.nodes
                .iter()
                .any(|node| node.variable.starts_with("__coral_hidden_person")),
            "{:?}",
            plan.nodes
        );
        assert!(
            plan.relationships
                .first()
                .is_some_and(|relationship| relationship.left.starts_with("__coral_hidden_person")),
            "{:?}",
            plan.relationships
        );
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
    fn compiles_transparent_with_rebound_dropped_variable_name() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WITH service \
             MATCH (person:Person)-[:OWNS]->(service) \
             RETURN person.name AS owner, service.name AS service",
        )
        .expect("dropped variable names should be reusable after transparent WITH");

        assert!(
            plan.nodes
                .iter()
                .any(|node| node.variable.starts_with("__coral_hidden_person")),
            "{:?}",
            plan.nodes
        );
        assert!(plan.nodes.iter().any(|node| node.variable == "person"));
        assert_eq!(plan.relationships.len(), 2);
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
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "person".to_string(),
                        property: "name".to_string(),
                    }),
                    direction: OrderDirection::Ascending,
                    nulls: None,
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
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    direction: OrderDirection::Ascending,
                    nulls: None,
                },
            ]
        );
    }

    #[test]
    fn compiles_terminal_with_reordered_final_return_aliases() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WITH service.tier AS tier, count(service) AS services \
             RETURN services AS total_services, tier AS service_tier \
             ORDER BY total_services DESC, service_tier",
        )
        .expect("terminal WITH final RETURN aliases should reorder projections");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Aggregate {
                    function: AggregateFunction::Count,
                    target: AggregateTarget::VariableKey {
                        variable: "service".to_string(),
                    },
                    distinct: false,
                    alias: "total_services".to_string(),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    },
                    alias: Some("service_tier".to_string()),
                },
            ]
        );
        assert_eq!(
            plan.order_by,
            vec![
                OrderKey {
                    expression: OrderExpression::ProjectionAlias("total_services".to_string()),
                    direction: OrderDirection::Descending,
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    direction: OrderDirection::Ascending,
                    nulls: None,
                },
            ]
        );
    }

    #[test]
    fn compiles_terminal_with_return_star_alias_passthrough() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             WITH service.tier AS tier, count(service) AS services \
             RETURN * \
             ORDER BY services DESC, tier",
        )
        .expect("terminal WITH RETURN * should pass through scalar aliases");

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
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    direction: OrderDirection::Ascending,
                    nulls: None,
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
                nulls: None,
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
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    direction: OrderDirection::Ascending,
                    nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
            }]
        );
    }

    #[test]
    fn compiles_relationship_endpoint_property_projections() {
        let plan = compile_cypher(
            "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN startNode(dependency).name AS source, endNode(dependency).name AS target \
             ORDER BY endNode(dependency).name",
        )
        .expect("relationship endpoint property projections should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "source".to_string(),
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
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            }]
        );
    }

    #[test]
    fn compiles_reversed_relationship_endpoint_property_projections() {
        let plan = compile_cypher(
            "MATCH (target:Service)<-[dependency:DEPENDS_ON]-(source:Service) \
             RETURN startNode(dependency).name AS source, endNode(dependency).name AS target",
        )
        .expect("reversed relationship endpoint property projections should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "source".to_string(),
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
    fn compiles_relationship_endpoint_properties_in_predicates_and_scalars() {
        let plan = compile_cypher(
            "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             WHERE startNode(dependency).tier = 'prod' \
             RETURN lower(endNode(dependency).name) AS target \
             ORDER BY endNode(dependency).name",
        )
        .expect("relationship endpoint property scalar expressions should compile");

        assert_eq!(
            plan.predicates,
            vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "source".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            }]
        );
        assert_eq!(
            plan.projections,
            vec![Projection::Expression {
                expression: ScalarExpression::ToLower {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "target".to_string(),
                        property: "name".to_string(),
                    })),
                },
                alias: "target".to_string(),
            }]
        );
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            }]
        );
    }

    #[test]
    fn compiles_relationship_endpoint_property_aggregates() {
        let plan = compile_cypher(
            "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN count(startNode(dependency).name) AS sources",
        )
        .expect("relationship endpoint aggregate target should compile");

        assert_eq!(
            plan.projections,
            vec![Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "source".to_string(),
                    property: "name".to_string(),
                }),
                distinct: false,
                alias: "sources".to_string(),
            }]
        );
    }

    #[test]
    fn compiles_relationship_endpoint_identity_projections() {
        let plan = compile_cypher(
            "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN id(startNode(dependency)) AS source_id, \
                    elementId(endNode(dependency)) AS target_element_id, \
                    labels(startNode(dependency)) AS source_labels, \
                    keys(endNode(dependency)) AS target_keys \
             ORDER BY id(startNode(dependency))",
        )
        .expect("relationship endpoint identity projections should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Key {
                    variable: "source".to_string(),
                    alias: "source_id".to_string(),
                },
                Projection::ElementId {
                    variable: "target".to_string(),
                    alias: "target_element_id".to_string(),
                },
                Projection::NodeLabels {
                    variable: "source".to_string(),
                    label: "Service".to_string(),
                    alias: "source_labels".to_string(),
                },
                Projection::PropertyKeys {
                    variable: "target".to_string(),
                    alias: "target_keys".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Key {
                    variable: "source".to_string(),
                },
                direction: OrderDirection::Ascending,
                nulls: None,
            }]
        );
    }

    #[test]
    fn compiles_relationship_endpoint_identity_aggregates() {
        let plan = compile_cypher(
            "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN count(endNode(dependency)) AS targets",
        )
        .expect("relationship endpoint identity aggregate should compile");

        assert_eq!(
            plan.projections,
            vec![Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::VariableKey {
                    variable: "target".to_string(),
                },
                distinct: false,
                alias: "targets".to_string(),
            }]
        );
    }

    #[test]
    fn compiles_relationship_endpoint_identity_predicates() {
        let plan = compile_cypher(
            "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             WHERE startNode(dependency) IS NOT NULL \
               AND endNode(dependency):Service \
               AND 'Service' IN labels(startNode(dependency)) \
               AND 'name' IN keys(endNode(dependency)) \
             RETURN target.name AS target",
        )
        .expect("relationship endpoint identity predicates should compile");

        assert!(matches!(
            plan.predicate,
            Some(PredicateExpression::And { .. })
        ));
    }

    #[test]
    fn rejects_relationship_endpoint_identity_functions_on_optional_relationships() {
        let error = compile_cypher(
            "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
             RETURN id(endNode(dependency)) AS dependency_id",
        )
        .expect_err(
            "relationship endpoint identity functions over optional relationships should reject",
        );

        assert!(
            error
                .to_string()
                .contains("endNode() over optional relationship variables is not supported yet"),
            "{error}"
        );
    }

    #[test]
    fn rejects_relationship_endpoint_properties_on_undirected_relationships() {
        let error = compile_cypher(
            "MATCH (left:Service)-[dependency:DEPENDS_ON]-(right:Service) \
             RETURN startNode(dependency).name AS source",
        )
        .expect_err("undirected relationship endpoint properties should be rejected");

        assert!(
            error
                .to_string()
                .contains("startNode() over undirected relationships is not supported yet"),
            "{error}"
        );
    }

    #[test]
    fn rejects_relationship_endpoint_properties_on_node_variables() {
        let error = compile_cypher(
            "MATCH (service:Service) \
             RETURN startNode(service).name AS source",
        )
        .expect_err("relationship endpoint functions should require relationship variables");

        assert!(
            error
                .to_string()
                .contains("startNode() argument 'service' is not a named relationship variable"),
            "{error}"
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
                nulls: None,
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
                nulls: None,
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
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::Key {
                        variable: "owns".to_string(),
                    },
                    direction: OrderDirection::Descending,
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::RelationshipType {
                        variable: "owns".to_string(),
                        relationship_type: "OWNS".to_string(),
                    },
                    direction: OrderDirection::Ascending,
                    nulls: None,
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
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::ElementId {
                        variable: "owns".to_string(),
                    },
                    direction: OrderDirection::Descending,
                    nulls: None,
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
                nulls: None,
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
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::PropertyKeys {
                        variable: "owns".to_string(),
                    },
                    direction: OrderDirection::Ascending,
                    nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
                nulls: None,
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
    fn compiles_relationship_type_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN coalesce(type(owns), 'missing') AS rel_type, \
                    CASE WHEN service.tier = 'prod' THEN type(owns) ELSE 'other' END AS rel_bucket \
             ORDER BY coalesce(type(owns), 'missing')",
        )
        .expect("relationship type scalar expressions should compile");

        let [
            Projection::Expression {
                expression:
                    ScalarExpression::Coalesce {
                        expressions: coalesce_expressions,
                    },
                alias: coalesce_alias,
            },
            Projection::Expression {
                expression:
                    ScalarExpression::Case {
                        alternatives,
                        else_expression,
                    },
                alias: case_alias,
            },
        ] = plan.projections.as_slice()
        else {
            panic!("expected scalar relationship type projections");
        };
        assert_eq!(coalesce_alias, "rel_type");
        assert_eq!(
            coalesce_expressions,
            &vec![
                ScalarExpression::RelationshipType {
                    variable: "owns".to_string(),
                    relationship_type: "OWNS".to_string(),
                },
                ScalarExpression::Literal(Literal::String("missing".to_string())),
            ]
        );
        assert_eq!(case_alias, "rel_bucket");
        let [alternative] = alternatives.as_slice() else {
            panic!("expected one CASE alternative");
        };
        assert_eq!(
            alternative.then,
            ScalarExpression::RelationshipType {
                variable: "owns".to_string(),
                relationship_type: "OWNS".to_string(),
            }
        );
        assert_eq!(
            else_expression.as_deref(),
            Some(&ScalarExpression::Literal(Literal::String(
                "other".to_string()
            )))
        );
        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Coalesce { expressions }),
                direction: OrderDirection::Ascending,
                nulls: None,
            }] if matches!(
                expressions.as_slice(),
                [
                    ScalarExpression::RelationshipType {
                        variable,
                        relationship_type,
                    },
                    ScalarExpression::Literal(Literal::String(fallback)),
                ] if variable == "owns" && relationship_type == "OWNS" && fallback == "missing"
            )
        ));
    }

    #[test]
    fn compiles_identity_scalar_expressions() {
        let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN id(service) + 1 AS next_service_id, \
                    coalesce(elementId(owns), 'missing') AS ownership_element_id, \
                    CASE WHEN service.tier = 'prod' THEN id(person) ELSE 0 END AS owner_id \
             ORDER BY toString(id(service)), coalesce(elementId(owns), 'missing')",
        )
        .expect("identity scalar expressions should compile");

        let [
            Projection::Expression {
                expression:
                    ScalarExpression::Arithmetic {
                        operator: ArithmeticOperator::Add,
                        left,
                        right,
                    },
                alias: next_alias,
            },
            Projection::Expression {
                expression:
                    ScalarExpression::Coalesce {
                        expressions: coalesce_expressions,
                    },
                alias: element_alias,
            },
            Projection::Expression {
                expression:
                    ScalarExpression::Case {
                        alternatives,
                        else_expression,
                    },
                alias: case_alias,
            },
        ] = plan.projections.as_slice()
        else {
            panic!("expected identity scalar projections");
        };
        assert_eq!(next_alias, "next_service_id");
        assert_eq!(
            left.as_ref(),
            &ScalarExpression::Key {
                variable: "service".to_string(),
            }
        );
        assert_eq!(
            right.as_ref(),
            &ScalarExpression::Literal(Literal::Integer(1))
        );
        assert_eq!(element_alias, "ownership_element_id");
        assert_eq!(
            coalesce_expressions,
            &vec![
                ScalarExpression::ElementId {
                    variable: "owns".to_string(),
                },
                ScalarExpression::Literal(Literal::String("missing".to_string())),
            ]
        );
        assert_eq!(case_alias, "owner_id");
        let [alternative] = alternatives.as_slice() else {
            panic!("expected one CASE alternative");
        };
        assert_eq!(
            alternative.then,
            ScalarExpression::Key {
                variable: "person".to_string(),
            }
        );
        assert_eq!(
            else_expression.as_deref(),
            Some(&ScalarExpression::Literal(Literal::Integer(0)))
        );
        assert!(matches!(
            plan.order_by.as_slice(),
            [
                OrderKey {
                    expression: OrderExpression::Scalar(ScalarExpression::ToString { expression }),
                    direction: OrderDirection::Ascending,
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::Scalar(ScalarExpression::Coalesce { expressions }),
                    direction: OrderDirection::Ascending,
                    nulls: None,
                },
            ] if matches!(expression.as_ref(), ScalarExpression::Key { variable } if variable == "service")
                && matches!(expressions.as_slice(), [
                    ScalarExpression::ElementId { variable },
                    ScalarExpression::Literal(Literal::String(fallback)),
                ] if variable == "owns" && fallback == "missing")
        ));
    }

    #[test]
    fn rejects_identity_scalar_expressions_on_unbound_variables() {
        let error = compile_cypher(
            "MATCH (service:Service) \
             RETURN coalesce(id(owner), 0) AS owner_id",
        )
        .expect_err("id() over an unbound variable should be rejected");

        assert!(
            error
                .to_string()
                .contains("id() argument 'owner' is not a bound graph variable"),
            "{error:?}"
        );
    }

    #[test]
    fn rejects_relationship_type_scalar_expressions_on_nodes() {
        let error = compile_cypher(
            "MATCH (service:Service) \
             RETURN coalesce(type(service), 'missing') AS relationship_type",
        )
        .expect_err("type() over a node variable should be rejected");

        assert!(
            error
                .to_string()
                .contains("type() argument 'service' is not a named relationship variable"),
            "{error:?}"
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
                "MATCH (service:Service) RETURN coalesce(labels(service), 'unknown') AS owner_team",
                "scalar function 'labels'",
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
                nulls: None,
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
    fn compiles_undirected_optional_match_local_predicates() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency_edge:DEPENDS_ON]-(dependency:Service) \
             WHERE dependency.tier = 'dev' \
             RETURN service.name AS service, dependency.name AS dependency",
        )
        .expect("undirected OPTIONAL MATCH predicate should compile");

        assert_eq!(plan.optional_relationships, vec![0]);
        assert_eq!(plan.predicates, Vec::new());
        assert_eq!(plan.optional_matches.len(), 1);
        let optional_match = plan
            .optional_matches
            .first()
            .expect("optional match scope should be present");
        assert_eq!(optional_match.relationship_indices, vec![0]);
        assert!(optional_match.predicate.is_some());
        let relationship = plan
            .relationships
            .first()
            .expect("optional relationship should be present");
        assert_eq!(relationship.direction, Direction::Undirected);
    }

    #[test]
    fn rejects_relationship_endpoint_properties_on_optional_relationships() {
        let error = compile_cypher(
            "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
             RETURN service.name AS service, endNode(dependency).name AS dependency",
        )
        .expect_err("relationship endpoint functions over optional relationships should reject");

        assert!(
            error
                .to_string()
                .contains("endNode() over optional relationship variables is not supported yet"),
            "{error}"
        );
    }

    #[test]
    fn rejects_unsupported_optional_match_shapes() {
        assert_unsupported("OPTIONAL MATCH (service:Service) RETURN service.name");
        assert_unsupported(
            "MATCH (service:Service) OPTIONAL MATCH (service)-[:DEPENDS_ON]->(target:Service)-[:DEPENDS_ON]->(next:Service) RETURN service.name, target.name, next.name",
        );
        assert_unsupported(
            "MATCH (service:Service) OPTIONAL MATCH (service)-[:DEPENDS_ON]->(target:Service)-[:DEPENDS_ON]->(next:Service) WHERE next.tier = 'prod' RETURN service.name",
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
            "MATCH (person:Person)-[:OWNS]->(service:Service) WITH service RETURN person.name",
        );
    }

    #[test]
    fn rejects_terminal_with_projection_boundaries_requiring_staging() {
        assert_unsupported("MATCH (service:Service) WITH service.name RETURN service.name");
        assert_unsupported("MATCH (service:Service) WITH service AS renamed RETURN renamed");
        assert_unsupported("MATCH (service:Service) WITH service.name AS service RETURN missing");
        assert_unsupported(
            "MATCH (service:Service) WITH service.name AS value, service.tier AS value RETURN value",
        );
        assert_unsupported(
            "MATCH (service:Service) WITH service.name AS name, service.tier AS tier RETURN name, name",
        );
        assert_unsupported(
            "MATCH (service:Service) WITH service.name AS name, service.tier AS tier RETURN name",
        );
        assert_unsupported(
            "MATCH (service:Service) WITH service.name AS service MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN service, target.name",
        );
        assert_unsupported(
            "MATCH (service:Service) WITH service.name AS service RETURN service ORDER BY service.name",
        );
        assert_unsupported(
            "MATCH (service:Service) WITH service.name AS service RETURN *, service",
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
            "MATCH (a:Service)-[:DEPENDS_ON*..3]->(b:Service) RETURN a.name",
            "MATCH (a:Service)-[:DEPENDS_ON]->{0,1}(b:Service) RETURN a.name",
            "MATCH (a:Service)-[:DEPENDS_ON]->{1,}(b:Service) RETURN a.name",
            "MATCH (a:Service)-[:DEPENDS_ON*9..9]->(b:Service) RETURN a.name",
            "MATCH (a:Service) OPTIONAL MATCH (a)-[:DEPENDS_ON*1..2]->(b:Service) RETURN a.name",
            "MATCH (a:Service)-[:DEPENDS_ON*2]->(b:Person) RETURN a.name",
            "MATCH (a:Service)-[r:DEPENDS_ON*2]->(b:Service) RETURN a.name",
        ] {
            assert_unsupported(cypher);
        }
    }

    #[test]
    fn compiles_bounded_variable_length_relationship_ranges_as_union_all() {
        let query = compile_cypher_query(
            "MATCH path = (a:Service)-[:DEPENDS_ON*1..3]->(b:Service) \
             RETURN a.name AS source, b.name AS target, length(path) AS hops \
             ORDER BY source, target, hops",
        )
        .expect("bounded relationship range should compile");

        let GraphQuery::Union(union) = query else {
            panic!("expected bounded relationship range to expand into a union query");
        };
        assert_eq!(union.branches.len(), 2);
        let first_branch = union.branches.first().expect("first range branch");
        let second_branch = union.branches.get(1).expect("second range branch");
        assert_eq!(union.first.relationships.len(), 1);
        assert_eq!(first_branch.plan.relationships.len(), 2);
        assert_eq!(second_branch.plan.relationships.len(), 3);
        assert!(union.branches.iter().all(|branch| branch.all));
        assert_eq!(path_length_projection_literal(&union.first), Some(1));
        assert_eq!(path_length_projection_literal(&first_branch.plan), Some(2));
        assert_eq!(path_length_projection_literal(&second_branch.plan), Some(3));
        assert_eq!(union.order_by.len(), 3);
    }

    #[test]
    fn compiles_bounded_gql_relationship_quantifiers_as_union_all() {
        let query = compile_cypher_query(
            "MATCH (a:Service)-[:DEPENDS_ON]->{1,2}(b:Service) \
             RETURN a.name AS source, b.name AS target",
        )
        .expect("bounded GQL relationship quantifier should compile");

        let GraphQuery::Union(union) = query else {
            panic!("expected bounded relationship quantifier to expand into a union query");
        };
        assert_eq!(union.first.relationships.len(), 1);
        assert_eq!(union.branches.len(), 1);
        assert_eq!(
            union
                .branches
                .first()
                .expect("first range branch")
                .plan
                .relationships
                .len(),
            2
        );
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
    fn compiles_exact_fixed_relationship_ranges_as_repeated_hops() {
        for cypher in [
            "MATCH (a:Service)-[:DEPENDS_ON*2]->(b:Service) RETURN a.name",
            "MATCH (a:Service)-[:DEPENDS_ON*2..2]->(b:Service) RETURN a.name",
            "MATCH (a:Service)-[:DEPENDS_ON]->{2}(b:Service) RETURN a.name",
            "MATCH (a:Service)-[:DEPENDS_ON]->{2,2}(b:Service) RETURN a.name",
        ] {
            let plan = compile_cypher(cypher).expect("exact fixed relationship should compile");

            assert_eq!(
                plan.nodes,
                vec![
                    NodePattern {
                        variable: "a".to_string(),
                        label: "Service".to_string(),
                    },
                    NodePattern {
                        variable: "b".to_string(),
                        label: "Service".to_string(),
                    },
                    NodePattern {
                        variable: "__coral_node_0_1".to_string(),
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
                        left: "a".to_string(),
                        direction: Direction::Outgoing,
                        right: "__coral_node_0_1".to_string(),
                    },
                    RelationshipPattern {
                        variable: None,
                        relationship_type: "DEPENDS_ON".to_string(),
                        left: "__coral_node_0_1".to_string(),
                        direction: Direction::Outgoing,
                        right: "b".to_string(),
                    },
                ]
            );
        }
    }

    #[test]
    fn compiles_exact_fixed_relationship_range_property_maps_per_hop() {
        let plan = compile_cypher(
            "MATCH (a:Service)-[:DEPENDS_ON*2 {source: 'catalog'}]->(b:Service) RETURN a.name",
        )
        .expect("exact fixed relationship property map should compile");

        assert_eq!(
            plan.relationships,
            vec![
                RelationshipPattern {
                    variable: Some("__coral_rel_0".to_string()),
                    relationship_type: "DEPENDS_ON".to_string(),
                    left: "a".to_string(),
                    direction: Direction::Outgoing,
                    right: "__coral_node_0_1".to_string(),
                },
                RelationshipPattern {
                    variable: Some("__coral_rel_1".to_string()),
                    relationship_type: "DEPENDS_ON".to_string(),
                    left: "__coral_node_0_1".to_string(),
                    direction: Direction::Outgoing,
                    right: "b".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.predicates,
            vec![
                PropertyPredicate {
                    property: PropertyRef {
                        variable: "__coral_rel_0".to_string(),
                        property: "source".to_string(),
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: PredicateRhs::Literal(Literal::String("catalog".to_string())),
                },
                PropertyPredicate {
                    property: PropertyRef {
                        variable: "__coral_rel_1".to_string(),
                        property: "source".to_string(),
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: PredicateRhs::Literal(Literal::String("catalog".to_string())),
                },
            ]
        );
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
    fn compiles_static_label_expression_patterns() {
        let plan = compile_cypher(
            "MATCH (person:Person&!Team)-[owns:OWNS&!DEPENDS_ON]->(service:Service&!Team) \
             RETURN person.name AS owner, service.name AS service",
        )
        .expect("static label expression patterns should compile");

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
                variable: Some("owns".to_string()),
                relationship_type: "OWNS".to_string(),
                left: "person".to_string(),
                direction: Direction::Outgoing,
                right: "service".to_string(),
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
    fn rejects_ambiguous_label_expression_patterns() {
        assert_unsupported("MATCH (service:Service|Team) RETURN service.name");
        assert_unsupported("MATCH (service:Service&Team) RETURN service.name");
        assert_unsupported("MATCH (service:Service&!Service) RETURN service.name");
        assert_unsupported("MATCH (service:!Team) RETURN service.name");
        assert_unsupported(
            "MATCH (person:Person)-[:OWNS|DEPENDS_ON]->(service:Service) RETURN service.name",
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
                nulls: None,
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
                nulls: None,
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
                    min(DISTINCT service.risk) AS distinct_lowest_risk, \
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
                    function: super::AggregateFunction::Min,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: true,
                    alias: "distinct_lowest_risk".to_string(),
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
                nulls: None,
            }]
        );
    }

    #[test]
    fn compiles_statistical_aggregate_projections() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN stDev(service.risk) AS sample_risk, \
                    stDevP(service.risk) AS population_risk",
        )
        .expect("statistical aggregate query should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Aggregate {
                    function: super::AggregateFunction::StdDev,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: false,
                    alias: "sample_risk".to_string(),
                },
                Projection::Aggregate {
                    function: super::AggregateFunction::StdDevP,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: false,
                    alias: "population_risk".to_string(),
                },
            ]
        );
    }

    #[test]
    fn rejects_distinct_standard_deviation_aggregate_projections() {
        let error = compile_cypher(
            "MATCH (service:Service) \
             RETURN stDevP(DISTINCT service.risk) AS population_risk",
        )
        .expect_err("distinct standard-deviation aggregate should be rejected");

        assert!(
            error.to_string().contains("UNSUPPORTED_CYPHER"),
            "{error:?}"
        );
        assert!(
            error.to_string().contains("stDevP(DISTINCT property)"),
            "{error:?}"
        );
    }

    #[test]
    fn compiles_median_aggregate_projections() {
        let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN median(service.risk) AS median_risk, \
                    median(DISTINCT service.risk) AS distinct_median_risk",
        )
        .expect("median aggregate query should compile");

        assert!(matches!(
            plan.projections.as_slice(),
            [
                Projection::Aggregate {
                    target: AggregateTarget::Property(PropertyRef {
                        variable,
                        property,
                    }),
                    distinct: false,
                    alias,
                    ..
                },
                Projection::Aggregate {
                    distinct: true,
                    alias: distinct_alias,
                    ..
                },
            ] if variable == "service"
                && property == "risk"
                && alias == "median_risk"
                && distinct_alias == "distinct_median_risk"
        ));
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
                nulls: None,
            }]
        );
    }

    #[test]
    fn rejects_order_by_unknown_aliases() {
        assert_unsupported("MATCH (service:Service) RETURN service.name AS name ORDER BY missing");
    }

    #[test]
    fn rejects_unsupported_return_functions() {
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
                nulls: None,
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
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    direction: OrderDirection::Ascending,
                    nulls: None,
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
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::ProjectionAlias("average_risk".to_string()),
                    direction: OrderDirection::Ascending,
                    nulls: None,
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
}
