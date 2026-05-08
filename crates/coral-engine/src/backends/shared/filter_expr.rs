//! Converts pushed-down `DataFusion` filters into manifest-defined source filters.

use std::collections::{HashMap, HashSet};

use datafusion::logical_expr::{Expr, Operator};
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
        collect_filter_values(expr, &allowed, &filter_modes, &mut filters);
    }

    filters
}

fn collect_filter_values(
    expr: &Expr,
    allowed: &HashSet<&str>,
    filter_modes: &HashMap<&str, FilterMode>,
    filters: &mut HashMap<String, String>,
) {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_filter_values(binary.left.as_ref(), allowed, filter_modes, filters);
            collect_filter_values(binary.right.as_ref(), allowed, filter_modes, filters);
        }
        Expr::Column(col) => {
            insert_bool_filter(col.name(), true, allowed, filters);
        }
        Expr::Not(inner) | Expr::IsFalse(inner) => {
            if let Expr::Column(col) = inner.as_ref() {
                insert_bool_filter(col.name(), false, allowed, filters);
            }
        }
        Expr::IsTrue(inner) => {
            if let Expr::Column(col) = inner.as_ref() {
                insert_bool_filter(col.name(), true, allowed, filters);
            }
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            if let Some((col, val)) =
                extract_column_equality(binary.left.as_ref(), binary.right.as_ref(), allowed)
            {
                filters.insert(col, val);
                return;
            }

            if let Some((col, val)) =
                extract_column_equality(binary.right.as_ref(), binary.left.as_ref(), allowed)
            {
                filters.insert(col, val);
            }
        }
        Expr::Like(like) if !like.negated => {
            if let Some((col, val)) = extract_column_like(
                like.expr.as_ref(),
                like.pattern.as_ref(),
                allowed,
                filter_modes,
            ) {
                filters.insert(col, val);
            }
        }
        Expr::InList(in_list) if !in_list.negated && in_list.list.len() == 1 => {
            let Expr::Column(col) = in_list.expr.as_ref() else {
                return;
            };
            let col_name = col.name().to_string();
            if !allowed.contains(col_name.as_str()) {
                return;
            }
            if let Some(value) = literal_to_string(&in_list.list[0]) {
                filters.insert(col_name, value);
            }
        }
        _ => {}
    }
}

fn insert_bool_filter(
    col_name: &str,
    value: bool,
    allowed: &HashSet<&str>,
    filters: &mut HashMap<String, String>,
) {
    if allowed.contains(col_name) {
        filters.insert(col_name.to_string(), value.to_string());
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

#[allow(
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

    #[test]
    fn extracts_required_filters_from_conjunctions() {
        let filters = vec![
            FilterSpec {
                name: "owner".into(),
                required: true,
                mode: FilterMode::default(),
            },
            FilterSpec {
                name: "status".into(),
                required: true,
                mode: FilterMode::default(),
            },
        ];

        let expr = equality_expr("owner", "alice").and(equality_expr("status", "open"));
        let values = extract_filter_values(&[expr], &filters);

        assert_eq!(values.get("owner").map(String::as_str), Some("alice"));
        assert_eq!(values.get("status").map(String::as_str), Some("open"));
    }

    #[test]
    fn extracts_single_item_in_list_as_constant_filter() {
        let filters = vec![FilterSpec {
            name: "repo".into(),
            required: false,
            mode: FilterMode::default(),
        }];

        let expr = col("repo").in_list(vec![lit("coral")], false);
        let values = extract_filter_values(&[expr], &filters);

        assert_eq!(values.get("repo").map(String::as_str), Some("coral"));
    }

    #[test]
    fn search_filter_also_accepts_equality() {
        let filters = vec![FilterSpec {
            name: "q".into(),
            required: false,
            mode: FilterMode::Search,
        }];

        let expr = equality_expr("q", "deploy");
        let values = extract_filter_values(&[expr], &filters);
        assert_eq!(values.get("q").map(String::as_str), Some("deploy"));
    }

    #[test]
    fn like_ignored_for_equality_mode_filter() {
        let filters = vec![FilterSpec {
            name: "q".into(),
            required: false,
            mode: FilterMode::Equality,
        }];

        let expr = like_expr("q", "%deploy%");
        let values = extract_filter_values(&[expr], &filters);
        assert!(values.is_empty());
    }

    #[test]
    fn strips_wildcards_from_like_pattern() {
        let filters = vec![FilterSpec {
            name: "q".into(),
            required: false,
            mode: FilterMode::Search,
        }];

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
    fn extracts_like_value_for_search_mode_filter() {
        let filters = vec![FilterSpec {
            name: "q".into(),
            required: false,
            mode: FilterMode::Search,
        }];

        let expr = like_expr("q", "%deploy%");
        let values = extract_filter_values(&[expr], &filters);

        assert_eq!(values.get("q").map(String::as_str), Some("deploy"));
    }

    #[test]
    fn extracts_boolean_true_from_bare_column_filter() {
        let filters = vec![FilterSpec {
            name: "descending".into(),
            required: false,
            mode: FilterMode::default(),
        }];

        let values = extract_filter_values(&[col("descending")], &filters);

        assert_eq!(values.get("descending").map(String::as_str), Some("true"));
    }

    #[test]
    fn extracts_boolean_false_from_not_column_filter() {
        let filters = vec![FilterSpec {
            name: "descending".into(),
            required: false,
            mode: FilterMode::default(),
        }];

        let values = extract_filter_values(&[col("descending").not()], &filters);

        assert_eq!(values.get("descending").map(String::as_str), Some("false"));
    }

    #[test]
    fn extracts_boolean_values_from_is_true_and_is_false_predicates() {
        let filters = vec![FilterSpec {
            name: "descending".into(),
            required: false,
            mode: FilterMode::default(),
        }];

        let cases = [
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
        let filters = vec![FilterSpec {
            name: "descending".into(),
            required: false,
            mode: FilterMode::default(),
        }];

        for expr in [
            Expr::IsNotTrue(Box::new(col("descending"))),
            Expr::IsNotFalse(Box::new(col("descending"))),
        ] {
            let values = extract_filter_values(&[expr], &filters);
            assert!(values.is_empty());
        }
    }
}
