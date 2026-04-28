//! Query-pattern validation for regex-style operators with common LIKE-wildcard
//! mistakes.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result as DataFusionResult, ScalarValue, plan_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::Operator;
use datafusion::logical_expr::expr::{BinaryExpr, Expr, Like};
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;

pub(crate) fn register_pattern_validator(
    registry: &mut dyn FunctionRegistry,
) -> DataFusionResult<()> {
    registry.register_function_rewrite(Arc::new(PatternValidator))?;
    Ok(())
}

#[derive(Debug)]
struct PatternValidator;

impl FunctionRewrite for PatternValidator {
    fn name(&self) -> &'static str {
        "PatternValidator"
    }

    fn rewrite(
        &self,
        expr: Expr,
        _schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> DataFusionResult<Transformed<Expr>> {
        validate_expr(&expr)?;
        Ok(Transformed::no(expr))
    }
}

fn validate_expr(expr: &Expr) -> DataFusionResult<()> {
    match expr {
        Expr::SimilarTo(like) => validate_similar_to(like),
        Expr::BinaryExpr(binary) => validate_regex_binary(binary),
        _ => Ok(()),
    }
}

fn validate_similar_to(like: &Like) -> DataFusionResult<()> {
    let Some(pattern) = extract_string_literal(&like.pattern) else {
        return Ok(());
    };

    if contains_like_wildcards(&pattern) {
        return plan_err!(
            "SIMILAR TO pattern '{pattern}' contains `%` or `_` which are literal characters in SIMILAR TO, not wildcards. Use `.*` instead of `%`, `.` instead of `_`, or use LIKE for wildcard matching."
        );
    }

    Ok(())
}

fn validate_regex_binary(binary: &BinaryExpr) -> DataFusionResult<()> {
    if !is_regex_operator(binary.op) {
        return Ok(());
    }

    let Some(pattern) = extract_string_literal(&binary.right) else {
        return Ok(());
    };

    if pattern.contains('%') {
        return plan_err!(
            "Regex operator `{}` pattern '{}' contains `%` which is a literal character in regex, not a wildcard. Use `.*` instead of `%`, or use LIKE/ILIKE for wildcard matching.",
            binary.op,
            pattern
        );
    }

    Ok(())
}

fn extract_string_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(
            ScalarValue::Utf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
            | ScalarValue::LargeUtf8(Some(value)),
            _,
        ) => Some(value.clone()),
        Expr::Alias(alias) => extract_string_literal(&alias.expr),
        _ => None,
    }
}

fn contains_like_wildcards(pattern: &str) -> bool {
    pattern.contains('%') || pattern.contains('_')
}

fn is_regex_operator(op: Operator) -> bool {
    matches!(
        op,
        Operator::RegexMatch
            | Operator::RegexIMatch
            | Operator::RegexNotMatch
            | Operator::RegexNotIMatch
    )
}

#[cfg(test)]
mod tests {
    use datafusion::common::config::ConfigOptions;
    use datafusion::common::{Column, DFSchema, ScalarValue};
    use datafusion::logical_expr::Operator;
    use datafusion::logical_expr::expr::{BinaryExpr, Expr, Like};
    use datafusion::logical_expr::expr_rewriter::FunctionRewrite;

    use super::PatternValidator;
    use crate::runtime::pattern_validator::{
        contains_like_wildcards, extract_string_literal, validate_expr,
    };

    #[test]
    fn similar_to_with_percent_returns_error() {
        let error = rewrite_err(similar_to_expr("(Slack|Weekly)%"));
        assert!(error.contains("SIMILAR TO pattern '(Slack|Weekly)%'"));
        assert!(error.contains("Use `.*` instead of `%`"));
    }

    #[test]
    fn similar_to_with_underscore_returns_error() {
        let error = rewrite_err(similar_to_expr("Slack_Weekly"));
        assert!(error.contains("SIMILAR TO pattern 'Slack_Weekly'"));
        assert!(error.contains("Use `.*` instead of `%`"));
        assert!(error.contains("`.` instead of `_`"));
    }

    #[test]
    fn similar_to_with_regex_syntax_passes() {
        assert_rewrite_passes(&similar_to_expr("(Slack|Weekly).*"));
    }

    #[test]
    fn similar_to_exact_match_passes() {
        assert_rewrite_passes(&similar_to_expr("default"));
    }

    #[test]
    fn regex_match_with_percent_returns_error() {
        let error = rewrite_err(regex_expr(Operator::RegexMatch, "(Slack|Weekly)%"));
        assert!(error.contains("Regex operator `~` pattern '(Slack|Weekly)%'"));
        assert!(error.contains("Use `.*` instead of `%`"));
    }

    #[test]
    fn regex_match_without_percent_passes() {
        assert_rewrite_passes(&regex_expr(Operator::RegexMatch, "(Slack|Weekly).*"));
    }

    #[test]
    fn regex_imatch_with_percent_returns_error() {
        let error = rewrite_err(regex_expr(Operator::RegexIMatch, "(Slack|Weekly)%"));
        assert!(error.contains("Regex operator `~*` pattern '(Slack|Weekly)%'"));
    }

    #[test]
    fn negated_regex_with_percent_returns_error() {
        let error = rewrite_err(regex_expr(Operator::RegexNotIMatch, "(Slack|Weekly)%"));
        assert!(error.contains("Regex operator `!~*` pattern '(Slack|Weekly)%'"));
    }

    #[test]
    fn like_with_percent_passes() {
        assert_rewrite_passes(&like_expr("%Slack%", false));
    }

    #[test]
    fn ilike_with_percent_passes() {
        assert_rewrite_passes(&like_expr("%SLACK%", true));
    }

    #[test]
    fn extract_string_literal_supports_utf8_variants() {
        assert_eq!(
            extract_string_literal(&Expr::Literal(ScalarValue::Utf8(Some("a".into())), None)),
            Some("a".into())
        );
        assert_eq!(
            extract_string_literal(&Expr::Literal(
                ScalarValue::LargeUtf8(Some("b".into())),
                None
            )),
            Some("b".into())
        );
        assert_eq!(
            extract_string_literal(&Expr::Literal(
                ScalarValue::Utf8View(Some("c".into())),
                None
            )),
            Some("c".into())
        );
    }

    #[test]
    fn contains_like_wildcards_only_flags_like_syntax() {
        assert!(contains_like_wildcards("Slack%"));
        assert!(contains_like_wildcards("Slack_"));
        assert!(!contains_like_wildcards("Slack.*"));
    }

    #[test]
    fn validate_expr_ignores_non_literal_regex_patterns() {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("name"))),
            Operator::RegexMatch,
            Box::new(Expr::Column(Column::from_name("pattern"))),
        ));

        validate_expr(&expr).expect("column-driven regex should not error");
    }

    fn similar_to_expr(pattern: &str) -> Expr {
        Expr::SimilarTo(Like::new(
            false,
            Box::new(Expr::Column(Column::from_name("name"))),
            Box::new(string_literal(pattern)),
            None,
            false,
        ))
    }

    fn like_expr(pattern: &str, case_insensitive: bool) -> Expr {
        Expr::Like(Like::new(
            false,
            Box::new(Expr::Column(Column::from_name("name"))),
            Box::new(string_literal(pattern)),
            None,
            case_insensitive,
        ))
    }

    fn regex_expr(op: Operator, pattern: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("name"))),
            op,
            Box::new(string_literal(pattern)),
        ))
    }

    fn string_literal(value: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(value.to_string())), None)
    }

    fn assert_rewrite_passes(expr: &Expr) {
        let result = PatternValidator
            .rewrite(expr.clone(), &DFSchema::empty(), &ConfigOptions::new())
            .expect("rewrite should pass");
        assert!(!result.transformed);
        assert_eq!(result.data, *expr);
    }

    fn rewrite_err(expr: Expr) -> String {
        PatternValidator
            .rewrite(expr, &DFSchema::empty(), &ConfigOptions::new())
            .expect_err("rewrite should fail")
            .to_string()
    }
}
