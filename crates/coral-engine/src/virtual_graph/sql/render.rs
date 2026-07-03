//! SQL text primitives for the graph-plan → `DataFusion` SQL `SqlRenderer`: stateless free
//! functions that quote identifiers and string literals; render literals, comparison
//! and arithmetic operators, aggregate invocations, LIKE/regex/XOR patterns, typed
//! literal lists and table references; and assemble UNION branch and outer-query SQL.
//! Unlike the sibling modules these are module-level helpers, not `SqlRenderer` methods.

use std::fmt::Write as _;

use super::{
    AggregateFunction, ComparisonOperator, CoreError, Diagnostic, GraphUnion,
    GraphUnionOuterProjectionItem, Literal, LiteralListElementType, NullOrder, OrderDirection,
    OrderExpression, Projection, ScalarExpression, TableRef,
};
use crate::virtual_graph::diagnostic_codes;

pub(super) fn scalar_expression_unary_operand(
    expression: &ScalarExpression,
) -> Option<&ScalarExpression> {
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
        | ScalarExpression::IsNaN { expression }
        | ScalarExpression::Negate { expression } => Some(expression),
        _ => None,
    }
}

pub(super) fn render_table_ref(table: &TableRef) -> String {
    format!(
        "{}.{}",
        quote_ident(&table.schema),
        quote_ident(&table.name)
    )
}

pub(super) fn render_union_branch_sql(sql: &str, index: usize) -> String {
    format!(
        "SELECT * FROM ({sql}) AS {}",
        quote_ident(&format!("__coral_union_b{index}"))
    )
}

pub(super) fn render_union_outer_sql(sql: String, union: &GraphUnion) -> Result<String, CoreError> {
    if union.outer_projection.is_none()
        && !union.distinct
        && union.order_by.is_empty()
        && union.skip.is_none()
        && union.limit.is_none()
    {
        return Ok(sql);
    }

    let distinct = if union.distinct { "DISTINCT " } else { "" };
    let projection = render_union_outer_projection(union);
    let mut outer_sql = format!(
        "SELECT {distinct}{projection} FROM ({sql}) AS {}",
        quote_ident("__coral_union_outer")
    );
    if let Some(outer_projection) = &union.outer_projection
        && !outer_projection.group_by.is_empty()
    {
        let groups = outer_projection
            .group_by
            .iter()
            .map(|column| quote_ident(column))
            .collect::<Vec<_>>()
            .join(", ");
        write!(outer_sql, " GROUP BY {groups}")
            .map_err(|_| CoreError::internal("failed to render graph union GROUP BY"))?;
    }
    if !union.order_by.is_empty() {
        let mut keys = Vec::with_capacity(union.order_by.len());
        for (index, key) in union.order_by.iter().enumerate() {
            let nulls = render_null_order(key.nulls);
            keys.push(format!(
                "{} {}{}",
                render_union_outer_order_expression(&key.expression, index)?,
                match key.direction {
                    OrderDirection::Ascending => "ASC",
                    OrderDirection::Descending => "DESC",
                },
                nulls,
            ));
        }
        write!(outer_sql, " ORDER BY {}", keys.join(", "))
            .map_err(|_| CoreError::internal("failed to render graph union SQL"))?;
    }
    if let Some(limit) = union.limit {
        write!(outer_sql, " LIMIT {limit}")
            .map_err(|_| CoreError::internal("failed to render graph union SQL"))?;
    }
    if let Some(skip) = union.skip {
        write!(outer_sql, " OFFSET {skip}")
            .map_err(|_| CoreError::internal("failed to render graph union SQL"))?;
    }
    Ok(outer_sql)
}

pub(super) fn render_null_order(nulls: Option<NullOrder>) -> &'static str {
    match nulls {
        Some(NullOrder::First) => " NULLS FIRST",
        Some(NullOrder::Last) => " NULLS LAST",
        None => "",
    }
}

fn render_union_outer_projection(union: &GraphUnion) -> String {
    let Some(outer_projection) = &union.outer_projection else {
        return "*".to_string();
    };
    outer_projection
        .items
        .iter()
        .map(render_union_outer_projection_item)
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_union_outer_projection_item(item: &GraphUnionOuterProjectionItem) -> String {
    match item {
        GraphUnionOuterProjectionItem::Column { name } => quote_ident(name),
        GraphUnionOuterProjectionItem::CountAll { alias } => {
            format!("COUNT(*) AS {}", quote_ident(alias))
        }
        GraphUnionOuterProjectionItem::Aggregate {
            function,
            source,
            distinct,
            alias,
        } => {
            let source = quote_ident(source);
            format!(
                "{} AS {}",
                render_aggregate_invocation_sql(*function, &source, *distinct),
                quote_ident(alias)
            )
        }
    }
}

fn render_union_outer_order_expression(
    expression: &OrderExpression,
    index: usize,
) -> Result<String, CoreError> {
    match expression {
        OrderExpression::ProjectionAlias(alias) => Ok(quote_ident(alias)),
        _ => Err(Diagnostic::new(
            diagnostic_codes::UNSUPPORTED_GRAPH_QUERY,
            format!("union.order_by[{index}].expression"),
            "graph union outer ORDER BY only supports projection aliases",
        )
        .into_core_error()),
    }
}

pub(super) fn validate_union_branch_output_names(
    expected: &[String],
    actual: &[String],
    branch_index: usize,
) -> Result<(), CoreError> {
    if expected == actual {
        return Ok(());
    }
    Err(Diagnostic::new(
        diagnostic_codes::UNION_SCHEMA_MISMATCH,
        format!("union.branches[{branch_index}].projections"),
        format!(
            "UNION branch projections must match the first branch; expected [{}], got [{}]",
            expected.join(", "),
            actual.join(", ")
        ),
    )
    .into_core_error())
}

pub(super) fn render_operator(operator: ComparisonOperator) -> &'static str {
    match operator {
        ComparisonOperator::Equal => "=",
        ComparisonOperator::NotEqual => "<>",
        ComparisonOperator::GreaterThan => ">",
        ComparisonOperator::GreaterThanOrEqual => ">=",
        ComparisonOperator::LessThan => "<",
        ComparisonOperator::LessThanOrEqual => "<=",
        ComparisonOperator::In => "IN",
        ComparisonOperator::StartsWith => "STARTS WITH",
        ComparisonOperator::EndsWith => "ENDS WITH",
        ComparisonOperator::Contains => "CONTAINS",
        ComparisonOperator::RegexMatch => {
            unreachable!("regex predicates lower through regexp_like")
        }
    }
}

fn render_aggregate_function(function: AggregateFunction) -> &'static str {
    match function {
        AggregateFunction::Count => "COUNT",
        AggregateFunction::Collect => "ARRAY_AGG",
        AggregateFunction::Sum => "SUM",
        AggregateFunction::Avg => "AVG",
        AggregateFunction::Median => "MEDIAN",
        AggregateFunction::PercentileCont { .. } => "PERCENTILE_CONT",
        AggregateFunction::StdDev => "STDDEV_SAMP",
        AggregateFunction::StdDevP => "STDDEV_POP",
        AggregateFunction::Min => "MIN",
        AggregateFunction::Max => "MAX",
    }
}

pub(super) fn render_aggregate_invocation_sql(
    function: AggregateFunction,
    target: &str,
    distinct: bool,
) -> String {
    let distinct_sql = if distinct { "DISTINCT " } else { "" };
    let target = if function == AggregateFunction::Median {
        format!("CAST({target} AS DOUBLE)")
    } else {
        target.to_string()
    };
    if let AggregateFunction::PercentileCont { percentile } = function {
        return format!(
            "PERCENTILE_CONT({distinct_sql}{target}, {})",
            render_float_literal(percentile.into_inner())
        );
    }
    if function == AggregateFunction::Collect {
        return format!(
            "COALESCE(ARRAY_AGG({distinct_sql}{target}) FILTER (WHERE ({target}) IS NOT NULL), make_array())"
        );
    }
    if distinct {
        match function {
            AggregateFunction::StdDev => {
                return format!("SQRT(VAR_SAMP(DISTINCT {target}))");
            }
            AggregateFunction::StdDevP => {
                return format!("SQRT(VAR_POP(DISTINCT {target}))");
            }
            _ => {}
        }
    }
    format!(
        "{}({distinct_sql}{target})",
        render_aggregate_function(function)
    )
}

pub(super) fn projection_output_alias(projection: &Projection) -> Option<&str> {
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

#[derive(Debug, Clone, Copy)]
pub(super) enum InfixArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

pub(super) fn render_arithmetic_operator(operator: InfixArithmeticOperator) -> &'static str {
    match operator {
        InfixArithmeticOperator::Add => "+",
        InfixArithmeticOperator::Subtract => "-",
        InfixArithmeticOperator::Multiply => "*",
        InfixArithmeticOperator::Divide => "/",
        InfixArithmeticOperator::Modulo => "%",
    }
}

pub(super) fn render_literal(literal: &Literal) -> String {
    match literal {
        Literal::String(value) => quote_string_literal(value),
        Literal::Integer(value) => value.to_string(),
        Literal::Float(value) => render_float_literal((*value).into_inner()),
        Literal::Boolean(value) => value.to_string(),
        Literal::Null => "NULL".to_string(),
    }
}

fn render_float_literal(value: f64) -> String {
    let rendered = value.to_string();
    if rendered.contains('.') || rendered.contains('e') || rendered.contains('E') {
        rendered
    } else {
        format!("{rendered}.0")
    }
}

pub(super) fn render_order_literal(literal: &Literal) -> String {
    match literal {
        Literal::Integer(_) => format!("CAST({} AS BIGINT)", render_literal(literal)),
        _ => render_literal(literal),
    }
}

pub(super) fn render_literal_list(literals: &[Literal]) -> String {
    let values = literals.iter().map(render_literal).collect::<Vec<_>>();
    render_sql_array(&values)
}

pub(super) fn render_sql_array(values: &[String]) -> String {
    let values = values.join(", ");
    format!("make_array({values})")
}

pub(super) fn render_typed_literal_list(
    literals: &[Literal],
    element_type: LiteralListElementType,
) -> String {
    if !literals.is_empty() {
        return render_literal_list(literals);
    }
    format!(
        "array_resize(make_array(CAST(NULL AS {})), 0)",
        render_literal_list_element_type(element_type)
    )
}

fn render_literal_list_element_type(element_type: LiteralListElementType) -> &'static str {
    match element_type {
        LiteralListElementType::String => "VARCHAR",
        LiteralListElementType::Integer => "BIGINT",
        LiteralListElementType::Float => "DOUBLE",
        LiteralListElementType::Boolean => "BOOLEAN",
    }
}

pub(super) fn render_like_pattern(operator: ComparisonOperator, value: &str) -> String {
    let escaped = escape_like_literal(value);
    let pattern = match operator {
        ComparisonOperator::StartsWith => format!("{escaped}%"),
        ComparisonOperator::EndsWith => format!("%{escaped}"),
        ComparisonOperator::Contains => format!("%{escaped}%"),
        _ => unreachable!("LIKE pattern requested for non-string predicate operator"),
    };
    quote_string_literal(&pattern)
}

pub(super) fn render_string_function_predicate(
    operator: ComparisonOperator,
    lhs: &str,
    rhs: &str,
) -> String {
    let function_name = match operator {
        ComparisonOperator::StartsWith => "starts_with",
        ComparisonOperator::EndsWith => "ends_with",
        ComparisonOperator::Contains => "contains",
        _ => unreachable!("string function requested for non-string predicate operator"),
    };
    format!("{function_name}({lhs}, {rhs})")
}

pub(super) fn render_regex_predicate(lhs: &str, rhs: &str) -> String {
    format!("regexp_like({lhs}, {rhs})")
}

pub(super) fn render_xor_predicate(left: &str, right: &str) -> String {
    format!("(({left} AND NOT ({right})) OR (NOT ({left}) AND {right}))")
}

fn escape_like_literal(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

pub(super) fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub(super) fn quote_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
