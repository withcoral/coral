//! `DataFusion` logical-plan fingerprints for trajectory memory.

use std::collections::BTreeSet;

use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, LogicalPlan};
use sha2::{Digest as _, Sha256};

use crate::QueryFingerprint;

/// Builds the structural query fingerprint from an optimized logical plan.
pub(crate) fn fingerprint_plan(plan: &LogicalPlan) -> DataFusionResult<QueryFingerprint> {
    let exact_plan = normalized_plan_text(plan);
    let shape_plan = abstract_literals(plan.clone())?;
    let shape_plan = normalized_plan_text(&shape_plan);
    Ok(QueryFingerprint::new(
        relations(plan),
        sha256_hex(&shape_plan),
        sha256_hex(&exact_plan),
    ))
}

fn relations(plan: &LogicalPlan) -> Vec<String> {
    let mut relations = BTreeSet::new();
    collect_relations(plan, &mut relations);
    relations.into_iter().collect()
}

fn collect_relations(plan: &LogicalPlan, relations: &mut BTreeSet<String>) {
    if let LogicalPlan::TableScan(scan) = plan {
        relations.insert(scan.table_name.to_string());
    }
    for input in plan.inputs() {
        collect_relations(input, relations);
    }
}

fn abstract_literals(plan: LogicalPlan) -> DataFusionResult<LogicalPlan> {
    let transformed = plan.transform(|plan| {
        plan.map_expressions(|expr| {
            expr.transform(|expr| match expr {
                Expr::Literal(value, metadata) => {
                    let placeholder = ScalarValue::try_new_null(&value.data_type())?;
                    Ok(Transformed::yes(Expr::Literal(placeholder, metadata)))
                }
                _ => Ok(Transformed::no(expr)),
            })
        })
    })?;
    Ok(transformed.data)
}

fn normalized_plan_text(plan: &LogicalPlan) -> String {
    format!("{}", plan.display_indent())
        .lines()
        .map(normalized_plan_line)
        .collect::<Vec<_>>()
        .join("\n")
}

fn normalized_plan_line(line: &str) -> String {
    let trimmed = line.trim_start();
    let indent = " ".repeat(line.len().saturating_sub(trimmed.len()));
    let Some((node, rest)) = trimmed.split_once(": ") else {
        return trimmed.to_string();
    };
    let normalized = match node {
        "Projection" => sorted_csv(rest),
        "Filter" => sorted_and_terms(rest),
        _ => rest.to_string(),
    };
    format!("{indent}{node}: {normalized}")
}

fn sorted_csv(value: &str) -> String {
    let mut parts = split_top_level(value, ',')
        .into_iter()
        .map(|part| part.trim().to_string())
        .collect::<Vec<_>>();
    parts.sort();
    parts.join(", ")
}

fn sorted_and_terms(value: &str) -> String {
    let mut parts = split_top_level_keyword(value, " AND ")
        .into_iter()
        .map(|part| part.trim().to_string())
        .collect::<Vec<_>>();
    parts.sort();
    parts.join(" AND ")
}

fn split_top_level(value: &str, delimiter: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0_i32;
    let mut in_string = false;
    let mut escaped = false;
    for (index, ch) in value.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '(' | '[' | '{' => depth = depth.saturating_add(1),
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            _ if ch == delimiter && depth == 0 => {
                if let Some(part) = value.get(start..index) {
                    parts.push(part.to_string());
                }
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    if let Some(part) = value.get(start..) {
        parts.push(part.to_string());
    }
    parts
}

fn split_top_level_keyword(value: &str, delimiter: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0_i32;
    let mut in_string = false;
    let mut escaped = false;
    let mut index = 0;
    while index < value.len() {
        let Some(remaining) = value.get(index..) else {
            break;
        };
        let Some(ch) = remaining.chars().next() else {
            break;
        };
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            index += ch.len_utf8();
            continue;
        }
        match ch {
            '"' => in_string = true,
            '(' | '[' | '{' => depth = depth.saturating_add(1),
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            _ if depth == 0 && remaining.starts_with(delimiter) => {
                if let Some(part) = value.get(start..index) {
                    parts.push(part.to_string());
                }
                index += delimiter.len();
                start = index;
                continue;
            }
            _ => {}
        }
        index += ch.len_utf8();
    }
    if let Some(part) = value.get(start..) {
        parts.push(part.to_string());
    }
    parts
}

fn sha256_hex(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::logical_plan::table_scan;
    use datafusion::logical_expr::{LogicalPlan, col, lit};

    use super::fingerprint_plan;

    fn plan(predicate_value: &str) -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("team", DataType::Utf8, false),
        ]);
        table_scan(Some("notion.search_objects"), &schema, None)
            .expect("table scan")
            .filter(
                col("name")
                    .eq(lit(predicate_value))
                    .and(col("team").eq(lit("hr"))),
            )
            .expect("filter")
            .project(vec![col("team"), col("name")])
            .expect("project")
            .build()
            .expect("build plan")
    }

    #[test]
    fn literals_change_exact_key_but_not_shape_hash() {
        let onboarding = fingerprint_plan(&plan("onboarding")).expect("fingerprint");
        let payroll = fingerprint_plan(&plan("payroll")).expect("fingerprint");

        assert_eq!(onboarding.shape_hash(), payroll.shape_hash());
        assert_ne!(onboarding.exact_key(), payroll.exact_key());
        assert_eq!(onboarding.relations(), ["notion.search_objects"]);
    }

    #[test]
    fn projection_and_predicate_order_are_canonicalized() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let left = table_scan(Some("demo.items"), &schema, None)
            .expect("table scan")
            .filter(col("a").eq(lit("x")).and(col("b").eq(lit("y"))))
            .expect("filter")
            .project(vec![col("a"), col("b")])
            .expect("project")
            .build()
            .expect("build plan");
        let right = table_scan(Some("demo.items"), &schema, None)
            .expect("table scan")
            .filter(col("b").eq(lit("y")).and(col("a").eq(lit("x"))))
            .expect("filter")
            .project(vec![col("b"), col("a")])
            .expect("project")
            .build()
            .expect("build plan");

        assert_eq!(
            fingerprint_plan(&left)
                .expect("left fingerprint")
                .exact_key(),
            fingerprint_plan(&right)
                .expect("right fingerprint")
                .exact_key()
        );
    }
}
