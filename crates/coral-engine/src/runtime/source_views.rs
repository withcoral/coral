//! Builds SQL-backed source view providers.

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{TableReference, tree_node::TreeNodeRecursion};
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{SQLOptions, SessionContext};

use crate::SourceTables;
use crate::backends::RegisteredTable;

pub(crate) struct SourceSqlView {
    metadata: RegisteredTable,
    sql: String,
}

impl SourceSqlView {
    pub(crate) fn new(metadata: RegisteredTable, sql: String) -> Self {
        Self { metadata, sql }
    }

    fn table_name(&self) -> &str {
        &self.metadata.table_name
    }
}

pub(crate) async fn build_source_views(
    ctx: &SessionContext,
    schema_name: &str,
    views: Vec<SourceSqlView>,
) -> Result<SourceTables> {
    let mut view_tables = SourceTables::new();
    for view in views {
        let plan = source_view_plan(ctx, schema_name, &view).await?;
        let provider = DataFrame::new(ctx.state(), plan).into_view();
        validate_source_view_schema(schema_name, &view, &provider.schema())?;
        if view_tables
            .insert(view.table_name().to_string(), provider)
            .is_some()
        {
            return Err(source_view_sql_error(
                schema_name,
                &view,
                "duplicate source view name",
            ));
        }
    }
    Ok(view_tables)
}

fn source_view_sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

async fn source_view_plan(
    ctx: &SessionContext,
    schema_name: &str,
    view: &SourceSqlView,
) -> Result<LogicalPlan> {
    let plan = ctx
        .state()
        .create_logical_plan(&view.sql)
        .await
        .map_err(|error| {
            DataFusionError::Plan(format!(
                "failed to plan view {schema_name}.{}: {error}",
                view.table_name()
            ))
        })?;
    source_view_sql_options()
        .verify_plan(&plan)
        .map_err(|error| source_view_sql_error(schema_name, view, error))?;
    validate_source_view_plan(schema_name, view, &plan)?;
    Ok(plan)
}

fn validate_source_view_plan(
    schema_name: &str,
    view: &SourceSqlView,
    plan: &LogicalPlan,
) -> Result<()> {
    let mut references_source_table = false;
    plan.apply_with_subqueries(|node| match node {
        LogicalPlan::TableScan(scan) => {
            let recursion = validate_source_table_reference(schema_name, view, &scan.table_name)?;
            references_source_table = true;
            Ok(recursion)
        }
        LogicalPlan::Analyze(_)
        | LogicalPlan::DescribeTable(_)
        | LogicalPlan::Explain(_)
        | LogicalPlan::Extension(_)
        | LogicalPlan::Unnest(_) => Err(source_view_sql_error(
            schema_name,
            view,
            format!(
                "unsupported logical plan node {}",
                logical_plan_node_name(node)
            ),
        )),
        _ => Ok(TreeNodeRecursion::Continue),
    })?;
    if !references_source_table {
        return Err(source_view_sql_error(
            schema_name,
            view,
            "view SQL must reference at least one source-local table",
        ));
    }
    Ok(())
}

fn validate_source_table_reference(
    schema_name: &str,
    view: &SourceSqlView,
    table_ref: &TableReference,
) -> Result<TreeNodeRecursion> {
    match table_ref {
        TableReference::Partial { schema, .. } if schema.as_ref() == schema_name => {
            Ok(TreeNodeRecursion::Continue)
        }
        TableReference::Partial { schema, table } => Err(source_view_sql_error(
            schema_name,
            view,
            format!("view SQL must reference only source-local tables; found {schema}.{table}"),
        )),
        TableReference::Bare { table } => Err(source_view_sql_error(
            schema_name,
            view,
            format!("view SQL must reference source tables as {schema_name}.table; found {table}"),
        )),
        TableReference::Full { .. } => Err(source_view_sql_error(
            schema_name,
            view,
            format!(
                "view SQL must reference source tables as {schema_name}.table; found {table_ref}"
            ),
        )),
    }
}

fn logical_plan_node_name(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Analyze(_) => "Analyze",
        LogicalPlan::DescribeTable(_) => "DescribeTable",
        LogicalPlan::Explain(_) => "Explain",
        LogicalPlan::Extension(_) => "Extension",
        LogicalPlan::Unnest(_) => "Unnest",
        _ => "Unknown",
    }
}

fn validate_source_view_schema(
    schema_name: &str,
    view: &SourceSqlView,
    actual_schema: &SchemaRef,
) -> Result<()> {
    let expected_columns = &view.metadata.columns;
    let actual_fields = actual_schema.fields();
    if expected_columns.len() != actual_fields.len() {
        return Err(source_view_sql_error(
            schema_name,
            view,
            format!(
                "declared columns do not match SQL output: expected {} columns, got {}",
                expected_columns.len(),
                actual_fields.len()
            ),
        ));
    }

    for (ordinal, (expected, actual)) in expected_columns
        .iter()
        .zip(actual_fields.iter())
        .enumerate()
    {
        if expected.name != *actual.name()
            || !view_data_type_matches(&expected.data_type, actual.data_type())
            || actual_allows_nulls_not_declared(expected.nullable, actual.is_nullable())
        {
            return Err(source_view_sql_error(
                schema_name,
                view,
                format!(
                    "declared columns do not match SQL output at position {}: expected {} {} nullable={}, got {} {} nullable={}",
                    ordinal + 1,
                    expected.name,
                    expected.data_type,
                    expected.nullable,
                    actual.name(),
                    actual.data_type(),
                    actual.is_nullable()
                ),
            ));
        }
    }

    Ok(())
}

fn actual_allows_nulls_not_declared(expected_nullable: bool, actual_nullable: bool) -> bool {
    !expected_nullable && actual_nullable
}

fn view_data_type_matches(expected: &str, actual: &DataType) -> bool {
    registered_data_type_matches(expected, actual) || expected == actual.to_string()
}

fn registered_data_type_matches(expected: &str, actual: &DataType) -> bool {
    match expected {
        "Utf8" | "Json" | "LargeUtf8" | "Utf8View" => is_string_type(actual),
        "Int64" => matches!(actual, DataType::Int64),
        "Boolean" => matches!(actual, DataType::Boolean),
        "Float64" => matches!(actual, DataType::Float64),
        "Timestamp" => matches!(actual, DataType::Timestamp(_, _)),
        _ => false,
    }
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn source_view_sql_error(
    schema_name: &str,
    view: &SourceSqlView,
    detail: impl std::fmt::Display,
) -> DataFusionError {
    DataFusionError::Plan(format!(
        "invalid view {schema_name}.{}: {detail}",
        view.table_name()
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::MemorySchemaProvider;
    use datafusion::common::TableReference;
    use datafusion::datasource::MemTable;
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;

    use super::{SourceSqlView, build_source_views, source_view_plan, validate_source_view_schema};
    use crate::CoreError;
    use crate::backends::RegisteredTable;
    use crate::backends::common::RegisteredColumn;
    use crate::runtime::error::datafusion_to_core;

    fn schema(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new(fields))
    }

    fn registered_table(table_name: &str, fields: Vec<Field>) -> RegisteredTable {
        RegisteredTable {
            table_name: table_name.to_string(),
            description: String::new(),
            guide: String::new(),
            columns: fields
                .into_iter()
                .map(|field| RegisteredColumn {
                    name: field.name().clone(),
                    data_type: field.data_type().to_string(),
                    nullable: field.is_nullable(),
                    is_virtual: false,
                    is_required_filter: false,
                    filter_mode: None,
                    description: String::new(),
                })
                .collect(),
            filters: vec![],
            required_filters: vec![],
            search_limits_json: None,
        }
    }

    fn source_view(sql: &str) -> SourceSqlView {
        SourceSqlView::new(
            registered_table("messages", vec![Field::new("text", DataType::Utf8, true)]),
            sql.to_string(),
        )
    }

    fn source_view_context() -> SessionContext {
        let ctx = SessionContext::new();
        let catalog = ctx.catalog("datafusion").expect("catalog should exist");
        catalog
            .register_schema("codex", Arc::new(MemorySchemaProvider::new()))
            .expect("codex schema should register");
        catalog
            .register_schema("github", Arc::new(MemorySchemaProvider::new()))
            .expect("github schema should register");
        register_test_table(&ctx, TableReference::partial("codex", "events"));
        register_test_table(&ctx, TableReference::partial("github", "issues"));
        register_test_table(&ctx, TableReference::bare("events"));
        ctx
    }

    fn register_test_table(ctx: &SessionContext, table_ref: TableReference) {
        let table_schema = schema(vec![Field::new("text", DataType::Utf8, true)]);
        let provider = Arc::new(
            MemTable::try_new(table_schema, vec![vec![]]).expect("mem table should build"),
        );
        ctx.register_table(table_ref, provider)
            .expect("test table should register");
    }

    async fn validate_source_view_sql(sql: &str) -> Result<()> {
        let ctx = source_view_context();
        let view = source_view(sql);
        source_view_plan(&ctx, "codex", &view).await.map(|_plan| ())
    }

    #[tokio::test]
    async fn source_view_accepts_source_qualified_relations_and_ctes() {
        let view = source_view("WITH recent AS (SELECT * FROM codex.events) SELECT * FROM recent");
        let ctx = source_view_context();

        source_view_plan(&ctx, "codex", &view)
            .await
            .expect("source-local view should validate");
    }

    #[tokio::test]
    async fn source_view_rejects_cross_source_relations() {
        let error = validate_source_view_sql("SELECT * FROM github.issues")
            .await
            .expect_err("cross-source view should fail validation");

        assert!(
            error
                .to_string()
                .contains("must reference only source-local tables"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn invalid_source_view_sql_maps_to_invalid_input() {
        let error = validate_source_view_sql("SELECT * FROM github.issues")
            .await
            .expect_err("cross-source view should fail validation");

        match datafusion_to_core(&error, &[]) {
            CoreError::InvalidInput(detail) => {
                assert!(
                    detail.contains("must reference only source-local tables"),
                    "unexpected error detail: {detail}"
                );
            }
            other => panic!("expected invalid input, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn source_view_rejects_cross_source_relations_in_subqueries() {
        let error = validate_source_view_sql(
            "SELECT (SELECT count(*) FROM github.issues) AS issue_count FROM codex.events",
        )
        .await
        .expect_err("cross-source subquery should fail validation");

        assert!(
            error
                .to_string()
                .contains("must reference only source-local tables"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn source_view_rejects_unqualified_base_relations() {
        let error = validate_source_view_sql("SELECT * FROM events")
            .await
            .expect_err("unqualified base table should fail validation");

        assert!(
            error
                .to_string()
                .contains("must reference source tables as codex.table"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn source_view_rejects_multiple_statements() {
        let error = validate_source_view_sql(
            "SELECT text FROM codex.events; SELECT text FROM codex.events",
        )
        .await
        .expect_err("multiple statements should fail validation");

        assert!(
            error
                .to_string()
                .contains("failed to plan view codex.messages"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn source_view_rejects_non_select_statements() {
        let error = validate_source_view_sql("DROP TABLE codex.events")
            .await
            .expect_err("non-select statement should fail validation");

        assert!(
            error.to_string().contains("DDL not supported"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn source_view_rejects_constant_selects() {
        let error = validate_source_view_sql("SELECT 'hello' AS text")
            .await
            .expect_err("constant view should fail validation");

        assert!(
            error
                .to_string()
                .contains("must reference at least one source-local table"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn source_view_rejects_unsupported_relation_forms() {
        let error = validate_source_view_sql("SELECT * FROM UNNEST([1, 2])")
            .await
            .expect_err("table functions should fail validation");

        assert!(
            error.to_string().contains("unsupported logical plan node"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn source_view_rejects_quoted_cross_source_relations() {
        let error = validate_source_view_sql(r#"SELECT * FROM "github".issues"#)
            .await
            .expect_err("quoted cross-source relation should fail validation");

        assert!(
            error
                .to_string()
                .contains("must reference only source-local tables"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn source_view_rejects_three_part_relations() {
        let error = validate_source_view_sql("SELECT * FROM datafusion.codex.events")
            .await
            .expect_err("three-part relation should fail validation");

        assert!(
            error
                .to_string()
                .contains("must reference source tables as codex.table"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn source_view_schema_accepts_matching_declared_columns() {
        let view = source_view("SELECT text FROM codex.events");
        let actual_schema = schema(vec![Field::new("text", DataType::Utf8, true)]);

        validate_source_view_schema("codex", &view, &actual_schema)
            .expect("matching schema should validate");
    }

    #[test]
    fn source_view_schema_accepts_stricter_actual_nullability() {
        let view = source_view("SELECT coalesce(text, '') AS text FROM codex.events");
        let actual_schema = schema(vec![Field::new("text", DataType::Utf8, false)]);

        validate_source_view_schema("codex", &view, &actual_schema)
            .expect("non-null SQL output should satisfy a nullable declaration");
    }

    #[test]
    fn source_view_schema_accepts_datafusion_string_view_output() {
        let view = source_view("SELECT CAST(NULL AS VARCHAR) AS text FROM codex.events");
        let actual_schema = schema(vec![Field::new("text", DataType::Utf8View, true)]);

        validate_source_view_schema("codex", &view, &actual_schema)
            .expect("DataFusion string view output should satisfy a Utf8 declaration");
    }

    #[test]
    fn source_view_schema_accepts_declared_json_string_output() {
        let mut view = source_view("SELECT payload AS text FROM codex.events");
        view.metadata
            .columns
            .first_mut()
            .expect("source_view helper creates one column")
            .data_type = "Json".to_string();
        let actual_schema = schema(vec![Field::new("text", DataType::Utf8, true)]);

        validate_source_view_schema("codex", &view, &actual_schema)
            .expect("declared JSON should satisfy a string-backed SQL output");
    }

    #[test]
    fn source_view_schema_rejects_looser_actual_nullability() {
        let mut view = source_view("SELECT maybe_text AS text FROM codex.events");
        view.metadata
            .columns
            .first_mut()
            .expect("source_view helper creates one column")
            .nullable = false;
        let actual_schema = schema(vec![Field::new("text", DataType::Utf8, true)]);

        let error = validate_source_view_schema("codex", &view, &actual_schema)
            .expect_err("nullable SQL output should not satisfy a non-null declaration");

        assert!(
            error
                .to_string()
                .contains("declared columns do not match SQL output at position 1"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn source_view_schema_rejects_declared_column_drift() {
        let view = source_view("SELECT body AS text FROM codex.events");
        let actual_schema = schema(vec![Field::new("body", DataType::Utf8, true)]);

        let error = validate_source_view_schema("codex", &view, &actual_schema)
            .expect_err("mismatched schema should fail validation");

        assert!(
            error
                .to_string()
                .contains("declared columns do not match SQL output at position 1"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn source_view_builds_queryable_provider() {
        let ctx = SessionContext::new();
        ctx.catalog("datafusion")
            .expect("catalog should exist")
            .register_schema("codex", Arc::new(MemorySchemaProvider::new()))
            .expect("source schema should register");

        let event_schema = schema(vec![Field::new("text", DataType::Utf8, true)]);
        let batch = RecordBatch::try_new(
            Arc::clone(&event_schema),
            vec![Arc::new(StringArray::from(vec!["hello"])) as ArrayRef],
        )
        .expect("batch should build");
        let provider = Arc::new(
            MemTable::try_new(Arc::clone(&event_schema), vec![vec![batch]])
                .expect("mem table should build"),
        );
        ctx.register_table(TableReference::partial("codex", "events"), provider)
            .expect("events table should register");
        let views = vec![SourceSqlView::new(
            registered_table("messages", vec![Field::new("text", DataType::Utf8, true)]),
            "SELECT text FROM codex.events".to_string(),
        )];

        let mut view_tables = build_source_views(&ctx, "codex", views)
            .await
            .expect("source view should build");
        let view_provider = view_tables
            .remove("messages")
            .expect("messages view provider should exist");
        ctx.register_table(TableReference::partial("codex", "messages"), view_provider)
            .expect("source view should register");

        let batches = ctx
            .sql("SELECT text FROM codex.messages")
            .await
            .expect("view query should plan")
            .collect()
            .await
            .expect("view query should execute");

        assert_single_text_value(&batches, "hello");
    }

    fn assert_single_text_value(batches: &[RecordBatch], expected: &str) {
        let [batch] = batches else {
            panic!("expected one batch, got {}", batches.len());
        };
        assert_eq!(batch.num_rows(), 1, "expected one row");
        let text = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("first column should be a string array");
        assert_eq!(text.value(0), expected);
    }
}
