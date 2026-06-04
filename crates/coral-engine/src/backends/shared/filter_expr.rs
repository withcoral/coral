//! Converts pushed-down `DataFusion` filters into manifest-defined source filters.

use std::collections::{HashMap, HashSet};

use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::scalar::ScalarValue;

use coral_spec::{FilterMode, FilterSpec};

/// Extracts manifest-defined filter values from pushed-down logical expressions.
pub(crate) fn extract_filter_values(
    exprs: &[Expr],
    defined_filters: &[FilterSpec],
) -> HashMap<String, String> {
    let allowed: HashSet<&str> = defined_filters.iter().map(|f| f.name.as_str()).collect();
    let filter_modes: HashMap<&str, FilterMode> = defined_filters
        .iter()
        .map(|f| (f.name.as_str(), f.mode))
        .collect();
    let mut filters = HashMap::new();

    for expr in exprs {
        analyze_filter(expr, &allowed, &filter_modes, Some(&mut filters));
    }

    filters
}

/// Classifies pushed-down logical expressions for `supports_filters_pushdown`,
/// mirroring [`extract_filter_values`] arm-for-arm so the pushdown decision and
/// the value extraction stay in lockstep.
pub(crate) fn classify_filter_pushdown(
    filters: &[&Expr],
    defined_filters: &[FilterSpec],
) -> Vec<TableProviderFilterPushDown> {
    let allowed: HashSet<&str> = defined_filters.iter().map(|f| f.name.as_str()).collect();
    let filter_modes: HashMap<&str, FilterMode> = defined_filters
        .iter()
        .map(|f| (f.name.as_str(), f.mode))
        .collect();

    filters
        .iter()
        .map(|expr| classify_filter(expr, &allowed, &filter_modes))
        .collect()
}

fn classify_filter(
    expr: &Expr,
    allowed: &HashSet<&str>,
    filter_modes: &HashMap<&str, FilterMode>,
) -> TableProviderFilterPushDown {
    analyze_filter(expr, allowed, filter_modes, None)
}

fn analyze_filter(
    expr: &Expr,
    allowed: &HashSet<&str>,
    filter_modes: &HashMap<&str, FilterMode>,
    mut filters: Option<&mut HashMap<String, String>>,
) -> TableProviderFilterPushDown {
    if let Expr::BinaryExpr(binary) = expr
        && binary.op == Operator::And
    {
        return classify_filter_conjunction(
            analyze_filter(
                binary.left.as_ref(),
                allowed,
                filter_modes,
                filters.as_deref_mut(),
            ),
            analyze_filter(binary.right.as_ref(), allowed, filter_modes, filters),
        );
    }
    if let Expr::Column(col) = expr
        && allowed.contains(col.name())
    {
        insert_filter_value(filters, col.name().to_string(), true.to_string());
        return TableProviderFilterPushDown::Exact;
    }
    if let Expr::Not(inner) = expr
        && let Expr::Column(col) = inner.as_ref()
        && allowed.contains(col.name())
    {
        insert_filter_value(filters, col.name().to_string(), false.to_string());
        return TableProviderFilterPushDown::Exact;
    }
    if let Expr::IsTrue(inner) | Expr::IsFalse(inner) = expr
        && let Expr::Column(col) = inner.as_ref()
        && allowed.contains(col.name())
    {
        insert_filter_value(
            filters,
            col.name().to_string(),
            matches!(expr, Expr::IsTrue(_)).to_string(),
        );
        return TableProviderFilterPushDown::Exact;
    }
    if let Expr::BinaryExpr(binary) = expr
        && binary.op == Operator::Eq
    {
        if let Some((col, val)) =
            extract_column_equality(binary.left.as_ref(), binary.right.as_ref(), allowed)
        {
            insert_filter_value(filters, col, val);
            return TableProviderFilterPushDown::Exact;
        }

        if let Some((col, val)) =
            extract_column_equality(binary.right.as_ref(), binary.left.as_ref(), allowed)
        {
            insert_filter_value(filters, col, val);
            return TableProviderFilterPushDown::Exact;
        }
    }
    if let Expr::Like(like) = expr
        && !like.negated
        && let Some((col, val)) = extract_column_like(
            like.expr.as_ref(),
            like.pattern.as_ref(),
            allowed,
            filter_modes,
        )
    {
        // Inexact: the API receives the stripped search/contains term (performance
        // win) but DataFusion keeps a residual filter to enforce exact
        // LIKE/ILIKE semantics client-side (correctness win).
        insert_filter_value(filters, col, val);
        return TableProviderFilterPushDown::Inexact;
    }
    if let Expr::InList(in_list) = expr
        && !in_list.negated
        && in_list.list.len() == 1
        && let Expr::Column(col) = in_list.expr.as_ref()
        && allowed.contains(col.name())
        && let Some(literal) = in_list.list.first()
        && let Some(value) = literal_to_string(literal)
    {
        insert_filter_value(filters, col.name().to_string(), value);
        return TableProviderFilterPushDown::Exact;
    }
    TableProviderFilterPushDown::Unsupported
}

fn insert_filter_value(
    filters: Option<&mut HashMap<String, String>>,
    column: String,
    value: String,
) {
    if let Some(filters) = filters {
        filters.insert(column, value);
    }
}

fn classify_filter_conjunction(
    left: TableProviderFilterPushDown,
    right: TableProviderFilterPushDown,
) -> TableProviderFilterPushDown {
    use TableProviderFilterPushDown::{Exact, Inexact, Unsupported};

    match (left, right) {
        (Unsupported, Unsupported) => Unsupported,
        (Exact, Exact) => Exact,
        _ => Inexact,
    }
}

fn extract_column_like(
    left: &Expr,
    right: &Expr,
    allowed: &HashSet<&str>,
    filter_modes: &HashMap<&str, FilterMode>,
) -> Option<(String, String)> {
    let Expr::Column(col) = left else {
        return None;
    };
    let col_name = col.name();
    if !allowed.contains(col_name) {
        return None;
    }
    let mode = filter_modes.get(col_name).copied().unwrap_or_default();
    if !matches!(mode, FilterMode::Search | FilterMode::Contains) {
        return None;
    }
    let raw = literal_to_string(right)?;
    let stripped = raw.strip_prefix('%').unwrap_or(&raw);
    let stripped = stripped.strip_suffix('%').unwrap_or(stripped);
    Some((col_name.to_string(), stripped.to_string()))
}

fn extract_column_equality(
    left: &Expr,
    right: &Expr,
    allowed: &HashSet<&str>,
) -> Option<(String, String)> {
    let Expr::Column(col) = left else {
        return None;
    };
    let col_name = col.name().to_string();
    if !allowed.contains(col_name.as_str()) {
        return None;
    }
    let value = literal_to_string(right)?;
    Some((col_name, value))
}

#[expect(
    clippy::match_same_arms,
    reason = "These match arms look similar but operate on different expression variants and value widths"
)]
pub(crate) fn literal_to_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(v)), _) => Some(v.clone()),
        Expr::Literal(ScalarValue::LargeUtf8(Some(v)), _) => Some(v.clone()),
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => Some(v.to_string()),
        Expr::Literal(ScalarValue::Int32(Some(v)), _) => Some(v.to_string()),
        Expr::Literal(ScalarValue::Float64(Some(v)), _) => Some(v.to_string()),
        Expr::Literal(ScalarValue::Float32(Some(v)), _) => Some(v.to_string()),
        Expr::Literal(ScalarValue::Boolean(Some(v)), _) => Some(v.to_string()),
        Expr::Cast(cast) => literal_to_string(cast.expr.as_ref()),
        Expr::TryCast(cast) => literal_to_string(cast.expr.as_ref()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::extract_filter_values;
    use coral_spec::{FilterMode, FilterSpec};
    use datafusion::logical_expr::{Expr, col, lit};
    use std::ops::Not;

    fn equality_expr(filter: &str, value: &str) -> Expr {
        col(filter).eq(lit(value))
    }

    fn like_expr(filter: &str, pattern: &str) -> Expr {
        Expr::Like(datafusion::logical_expr::Like {
            negated: false,
            expr: Box::new(col(filter)),
            pattern: Box::new(lit(pattern)),
            escape_char: None,
            case_insensitive: false,
        })
    }

    fn filter(name: &str, required: bool, mode: FilterMode) -> FilterSpec {
        FilterSpec {
            name: name.into(),
            data_type: "Utf8".into(),
            required,
            mode,
            description: String::new(),
        }
    }

    #[test]
    fn extracts_required_filters_from_conjunctions() {
        let filters = vec![
            filter("owner", true, FilterMode::default()),
            filter("status", true, FilterMode::default()),
        ];

        let expr = equality_expr("owner", "alice").and(equality_expr("status", "open"));
        let values = extract_filter_values(&[expr], &filters);

        assert_eq!(values.get("owner").map(String::as_str), Some("alice"));
        assert_eq!(values.get("status").map(String::as_str), Some("open"));
    }

    #[test]
    fn extracts_single_item_in_list_as_constant_filter() {
        let filters = vec![filter("repo", false, FilterMode::default())];

        let expr = col("repo").in_list(vec![lit("coral")], false);
        let values = extract_filter_values(&[expr], &filters);

        assert_eq!(values.get("repo").map(String::as_str), Some("coral"));
    }

    #[test]
    fn contains_filter_also_accepts_equality() {
        let filters = vec![filter("q", false, FilterMode::Contains)];

        let expr = equality_expr("q", "deploy");
        let values = extract_filter_values(&[expr], &filters);
        assert_eq!(values.get("q").map(String::as_str), Some("deploy"));
    }

    #[test]
    fn like_ignored_for_equality_mode_filter() {
        let filters = vec![filter("q", false, FilterMode::Equality)];

        let expr = like_expr("q", "%deploy%");
        let values = extract_filter_values(&[expr], &filters);
        assert!(values.is_empty());
    }

    #[test]
    fn strips_wildcards_from_like_pattern() {
        let filters = vec![filter("q", false, FilterMode::Contains)];

        let values = extract_filter_values(&[like_expr("q", "%deploy")], &filters);
        assert_eq!(values.get("q").map(String::as_str), Some("deploy"));

        let values = extract_filter_values(&[like_expr("q", "deploy%")], &filters);
        assert_eq!(values.get("q").map(String::as_str), Some("deploy"));

        let values = extract_filter_values(&[like_expr("q", "%deploy runbook%")], &filters);
        assert_eq!(values.get("q").map(String::as_str), Some("deploy runbook"));

        let values = extract_filter_values(&[like_expr("q", "exact")], &filters);
        assert_eq!(values.get("q").map(String::as_str), Some("exact"));
    }

    #[test]
    fn extracts_like_value_for_searchable_filter_modes() {
        for mode in [FilterMode::Contains, FilterMode::Search] {
            let filters = vec![filter("q", false, mode)];
            let values = extract_filter_values(&[like_expr("q", "%deploy%")], &filters);
            assert_eq!(values.get("q").map(String::as_str), Some("deploy"));
        }
    }

    #[test]
    fn extracts_boolean_values_from_column_predicates() {
        let filters = vec![filter("descending", false, FilterMode::default())];

        let cases = [
            (col("descending"), "true"),
            (col("descending").not(), "false"),
            (Expr::IsTrue(Box::new(col("descending"))), "true"),
            (Expr::IsFalse(Box::new(col("descending"))), "false"),
        ];

        for (expr, expected) in cases {
            let values = extract_filter_values(&[expr], &filters);
            assert_eq!(values.get("descending").map(String::as_str), Some(expected));
        }
    }

    #[test]
    fn ignores_null_inclusive_boolean_is_predicates() {
        let filters = vec![filter("descending", false, FilterMode::default())];

        for expr in [
            Expr::IsNotTrue(Box::new(col("descending"))),
            Expr::IsNotFalse(Box::new(col("descending"))),
        ] {
            let values = extract_filter_values(&[expr], &filters);
            assert!(values.is_empty());
        }
    }
}

#[cfg(test)]
mod pushdown_classification_tests {
    use super::classify_filter;
    use coral_spec::FilterMode;
    use datafusion::common::Column;
    use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, binary_expr, lit};
    use std::collections::{HashMap, HashSet};

    fn allowed<'a>(names: &'a [&'a str]) -> HashSet<&'a str> {
        names.iter().copied().collect()
    }

    fn modes<'a>(entries: &'a [(&'a str, FilterMode)]) -> HashMap<&'a str, FilterMode> {
        entries.iter().copied().collect()
    }

    fn col(name: &str) -> Expr {
        Expr::Column(Column::from_name(name))
    }

    #[test]
    fn reversed_equality_filter_pushes_down_exactly() {
        let pushdown = classify_filter(
            &binary_expr(lit("deploy"), Operator::Eq, col("query")),
            &allowed(&["query"]),
            &modes(&[]),
        );
        assert_eq!(pushdown, TableProviderFilterPushDown::Exact);
    }

    #[test]
    fn single_value_in_list_filter_pushes_down_exactly() {
        let pushdown = classify_filter(
            &col("repo").in_list(vec![lit("coral")], false),
            &allowed(&["repo"]),
            &modes(&[]),
        );
        assert_eq!(pushdown, TableProviderFilterPushDown::Exact);
    }

    #[test]
    fn conjunction_of_extractable_filters_pushes_down_exactly() {
        let pushdown = classify_filter(
            &binary_expr(col("owner"), Operator::Eq, lit("alice")).and(binary_expr(
                lit("open"),
                Operator::Eq,
                col("status"),
            )),
            &allowed(&["owner", "status"]),
            &modes(&[]),
        );
        assert_eq!(pushdown, TableProviderFilterPushDown::Exact);
    }

    #[test]
    fn partial_conjunction_pushdown_remains_inexact() {
        let pushdown = classify_filter(
            &binary_expr(col("owner"), Operator::Eq, lit("alice")).and(binary_expr(
                col("unmanaged"),
                Operator::Eq,
                lit("open"),
            )),
            &allowed(&["owner"]),
            &modes(&[]),
        );
        assert_eq!(pushdown, TableProviderFilterPushDown::Inexact);
    }

    #[test]
    fn null_inclusive_boolean_is_predicates_are_not_pushed_down() {
        for expr in [
            Expr::IsNotTrue(Box::new(col("descending"))),
            Expr::IsNotFalse(Box::new(col("descending"))),
        ] {
            let pushdown = classify_filter(&expr, &allowed(&["descending"]), &modes(&[]));
            assert_eq!(pushdown, TableProviderFilterPushDown::Unsupported);
        }
    }
}
