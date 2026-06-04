//! SQL parameter binding coverage.

use std::collections::BTreeMap;

use coral_engine::{CoralQuery, SqlParameterValue, SqlParameters};
use serde_json::json;

use crate::harness::{execution_to_rows, test_runtime};

#[tokio::test]
async fn positional_sql_parameters_bind_to_datafusion_placeholders() {
    let params = SqlParameters::Positional(vec![
        SqlParameterValue::Int64(41),
        SqlParameterValue::Utf8("Grace".to_string()),
        SqlParameterValue::Boolean(true),
        SqlParameterValue::Float64(1.5),
    ]);

    let execution = CoralQuery::execute_sql_with_params(
        &[],
        test_runtime(),
        "SELECT $1 + 1 AS n, $2 AS name, $3 AS active, $4 * 2.0 AS score",
        Some(&params),
    )
    .await
    .expect("parameterized query should succeed");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({
            "n": 42,
            "name": "Grace",
            "active": true,
            "score": 3.0
        })]
    );
}

#[tokio::test]
async fn named_sql_parameters_bind_without_leading_dollar() {
    let params = SqlParameters::Named(BTreeMap::from([
        (
            "name".to_string(),
            SqlParameterValue::Utf8("Ada".to_string()),
        ),
        ("n".to_string(), SqlParameterValue::Int64(7)),
    ]));

    let execution = CoralQuery::execute_sql_with_params(
        &[],
        test_runtime(),
        "SELECT $name AS name, $n + $n AS doubled",
        Some(&params),
    )
    .await
    .expect("parameterized query should succeed");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({
            "name": "Ada",
            "doubled": 14
        })]
    );
}

#[tokio::test]
async fn null_sql_parameter_can_be_cast_to_a_concrete_type() {
    let params = SqlParameters::Positional(vec![SqlParameterValue::Null]);

    let execution = CoralQuery::execute_sql_with_params(
        &[],
        test_runtime(),
        "SELECT COALESCE(CAST($1 AS BIGINT), 0) AS value",
        Some(&params),
    )
    .await
    .expect("parameterized query should succeed");

    assert_eq!(execution_to_rows(&execution), vec![json!({ "value": 0 })]);
}

#[tokio::test]
async fn float64_sql_parameter_preserves_float_type_for_whole_number_values() {
    let params = SqlParameters::Positional(vec![SqlParameterValue::Float64(1.0)]);

    let execution = CoralQuery::execute_sql_with_params(
        &[],
        test_runtime(),
        "SELECT $1 AS score",
        Some(&params),
    )
    .await
    .expect("whole-number float parameter should succeed");

    assert_eq!(
        execution.schema().first().expect("score field").data_type,
        "Float64"
    );
    assert_eq!(execution_to_rows(&execution), vec![json!({ "score": 1.0 })]);
}

#[tokio::test]
async fn numeric_sql_parameter_in_order_by_is_not_treated_as_select_ordinal() {
    let params = SqlParameters::Positional(vec![SqlParameterValue::Int64(2)]);

    let execution = CoralQuery::execute_sql_with_params(
        &[],
        test_runtime(),
        "SELECT * FROM (VALUES (1, 2), (2, 1)) AS t(a, b) ORDER BY $1",
        Some(&params),
    )
    .await
    .expect("numeric ORDER BY parameter should succeed");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({ "a": 1, "b": 2 }), json!({ "a": 2, "b": 1 })]
    );
}

#[tokio::test]
async fn missing_sql_parameter_returns_deterministic_error() {
    let error = CoralQuery::execute_sql_with_params(
        &[],
        test_runtime(),
        "SELECT $1 AS missing",
        Some(&SqlParameters::Positional(Vec::new())),
    )
    .await
    .expect_err("missing parameter should fail");

    assert!(
        error.to_string().contains("$1") || error.to_string().contains("placeholder"),
        "error should mention placeholder binding, got: {error}"
    );
}

#[tokio::test]
async fn engine_rejects_invalid_named_sql_parameter_contract() {
    for params in [
        SqlParameters::Named(BTreeMap::from([(
            String::new(),
            SqlParameterValue::Int64(1),
        )])),
        SqlParameters::Named(BTreeMap::from([(
            "$bad".to_string(),
            SqlParameterValue::Int64(1),
        )])),
    ] {
        let error =
            CoralQuery::execute_sql_with_params(&[], test_runtime(), "SELECT 1", Some(&params))
                .await
                .expect_err("invalid named parameter should fail");

        assert!(
            error.to_string().contains("named SQL parameter names"),
            "error should describe named parameter contract, got: {error}"
        );
    }
}

#[tokio::test]
async fn engine_rejects_non_finite_float_sql_parameter() {
    let error = CoralQuery::execute_sql_with_params(
        &[],
        test_runtime(),
        "SELECT $1",
        Some(&SqlParameters::Positional(vec![
            SqlParameterValue::Float64(f64::NAN),
        ])),
    )
    .await
    .expect_err("non-finite parameter should fail");

    assert!(
        error
            .to_string()
            .contains("float64 SQL parameters must be finite"),
        "error should describe float contract, got: {error}"
    );
}
