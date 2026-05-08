//! Validates regex-style query patterns to catch common LIKE-wildcard mistakes.
//!
//! `DataFusion`'s `SIMILAR TO` is implemented as a pure regex match — it maps
//! directly to `RegexMatch` without converting SQL-standard wildcards (`%`, `_`)
//! to their regex equivalents (`.*`, `.`). This means `WHERE name SIMILAR TO
//! 'Slack%'` silently matches nothing instead of behaving like `PostgreSQL`.
//!
//! See: <https://github.com/apache/datafusion/blob/eae7bf4/datafusion/physical-expr/src/expressions/binary.rs#L970-L983>
//!
//! This module registers a `FunctionRewrite` that returns a clear error when
//! `SIMILAR TO` patterns contain unescaped `%` or `_`. Escaped forms (`\%`,
//! `\_`) are allowed through since they work as literal matches in the
//! underlying regex engine.
//!
//! Regex operators (`~`, `~*`, `!~`, `!~*`) are not validated because `%` and
//! `_` are ordinary literal characters in regex and have legitimate uses
//! (e.g. matching "50%" or "`user_name`").

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result as DataFusionResult, ScalarValue, plan_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr::{Expr, Like};
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
        _ => Ok(()),
    }
}

fn validate_similar_to(like: &Like) -> DataFusionResult<()> {
    let Some(pattern) = extract_string_literal(&like.pattern) else {
        return Ok(());
    };

    if contains_unescaped_like_wildcards(&pattern) {
        return plan_err!(
            "SIMILAR TO pattern '{pattern}' contains `%` or `_` which are literal characters in SIMILAR TO, not wildcards. Use `.*` instead of `%`, `.` instead of `_`, use LIKE for wildcard matching, or escape with `\\%` / `\\_` if you want the literal character."
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

/// Returns `true` when `pattern` contains unescaped `%` or `_`.
///
/// A preceding backslash (`\%`, `\_`) signals the user intentionally wants the
/// literal character, so those occurrences are ignored.
fn contains_unescaped_like_wildcards(pattern: &str) -> bool {
    has_unescaped_char(pattern, '%') || has_unescaped_char(pattern, '_')
}

/// Returns `true` when `pattern` contains an unescaped occurrence of `ch`.
fn has_unescaped_char(pattern: &str, ch: char) -> bool {
    let mut prev_backslash = false;
    for c in pattern.chars() {
        if c == ch && !prev_backslash {
            return true;
        }
        prev_backslash = c == '\\';
    }
    false
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
        contains_unescaped_like_wildcards, extract_string_literal,
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
    fn similar_to_with_escaped_percent_passes() {
        assert_rewrite_passes(&similar_to_expr(r"100\%"));
    }

    #[test]
    fn similar_to_with_escaped_underscore_passes() {
        assert_rewrite_passes(&similar_to_expr(r"incident\_io"));
    }

    #[test]
    fn regex_match_with_percent_passes() {
        // % is a valid literal character in regex — no error
        assert_rewrite_passes(&regex_expr(Operator::RegexMatch, "(Slack|Weekly)%"));
    }

    #[test]
    fn regex_match_with_underscore_passes() {
        assert_rewrite_passes(&regex_expr(Operator::RegexMatch, "user_name"));
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
    fn contains_unescaped_like_wildcards_only_flags_unescaped() {
        assert!(contains_unescaped_like_wildcards("Slack%"));
        assert!(contains_unescaped_like_wildcards("Slack_"));
        assert!(!contains_unescaped_like_wildcards("Slack.*"));
        assert!(!contains_unescaped_like_wildcards(r"Slack\%"));
        assert!(!contains_unescaped_like_wildcards(r"Slack\_"));
        assert!(!contains_unescaped_like_wildcards(r"incident\_io"));
        assert!(contains_unescaped_like_wildcards(r"incident_io"));
        assert!(!contains_unescaped_like_wildcards(r"100\%"));
        assert!(contains_unescaped_like_wildcards(r"100%"));
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
