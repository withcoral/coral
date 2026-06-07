use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

/// Describes one queryable column.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub is_virtual: bool,
    pub is_required_filter: bool,
    pub description: String,
    pub ordinal_position: u32,
}

/// Describes one queryable table.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub schema_name: String,
    pub table_name: String,
    pub description: String,
    pub guide: String,
    pub columns: Vec<ColumnInfo>,
    pub required_filters: Vec<String>,
}

/// Describes one source-scoped table function.
#[derive(Debug, Clone)]
pub struct TableFunctionInfo {
    pub schema_name: String,
    pub function_name: String,
    pub description: String,
    pub arguments: Vec<TableFunctionArgumentInfo>,
    pub result_columns: Vec<TableFunctionResultColumnInfo>,
}

/// Describes one argument accepted by a source-scoped table function.
#[derive(Debug, Clone)]
pub struct TableFunctionArgumentInfo {
    pub name: String,
    pub required: bool,
    pub values: Vec<String>,
}

/// Describes one result column returned by a source-scoped table function.
#[derive(Debug, Clone)]
pub struct TableFunctionResultColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub description: String,
}

/// Describes SQL projections exposed by one runtime snapshot.
#[derive(Debug, Clone)]
pub struct SqlMetadataInfo {
    pub tables: Vec<TableInfo>,
    pub table_functions: Vec<TableFunctionInfo>,
}

/// Result of a table lookup from one runtime snapshot.
#[derive(Debug, Clone)]
pub struct SqlTableLookup {
    pub table: Option<TableInfo>,
    pub missing_context_tables: Vec<TableInfo>,
}

/// Query-engine plan renderings for one SQL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlan {
    unoptimized_logical: String,
    optimized_logical: String,
    physical: String,
}

impl QueryPlan {
    /// Builds one query-plan snapshot from plan renderings.
    #[must_use]
    pub fn new(
        unoptimized_logical_plan: String,
        optimized_logical_plan: String,
        physical_plan: String,
    ) -> Self {
        Self {
            unoptimized_logical: unoptimized_logical_plan,
            optimized_logical: optimized_logical_plan,
            physical: physical_plan,
        }
    }

    #[must_use]
    pub fn unoptimized_logical_plan(&self) -> &str {
        &self.unoptimized_logical
    }

    #[must_use]
    pub fn optimized_logical_plan(&self) -> &str {
        &self.optimized_logical
    }

    #[must_use]
    pub fn physical_plan(&self) -> &str {
        &self.physical
    }
}

/// The fully materialized result of executing one SQL statement.
#[derive(Debug, Clone)]
pub struct QueryExecution {
    schema: Vec<ColumnInfo>,
    arrow_schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
    row_count: usize,
}

impl QueryExecution {
    /// Builds a validated fully materialized query result.
    #[must_use]
    pub fn new(arrow_schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Self {
        let schema = arrow_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(position, field)| ColumnInfo {
                name: field.name().clone(),
                data_type: field.data_type().to_string(),
                nullable: field.is_nullable(),
                is_virtual: false,
                is_required_filter: false,
                description: String::new(),
                ordinal_position: u32::try_from(position).unwrap_or(u32::MAX),
            })
            .collect();
        let row_count = batches.iter().map(RecordBatch::num_rows).sum();
        Self {
            schema,
            arrow_schema,
            batches,
            row_count,
        }
    }

    #[must_use]
    pub fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    #[must_use]
    pub fn arrow_schema(&self) -> &Arc<Schema> {
        &self.arrow_schema
    }

    #[must_use]
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    #[must_use]
    pub fn row_count(&self) -> usize {
        self.row_count
    }
}

/// One validation query result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTestResult {
    sql: String,
    result: std::result::Result<QueryTestSuccess, QueryTestFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTestSuccess {
    row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTestFailure {
    error_message: String,
}

impl QueryTestSuccess {
    #[must_use]
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }
}

impl QueryTestFailure {
    #[must_use]
    pub fn error_message(&self) -> &str {
        &self.error_message
    }
}

impl QueryTestResult {
    #[must_use]
    pub fn success(sql: impl Into<String>, row_count: u64) -> Self {
        Self {
            sql: sql.into(),
            result: Ok(QueryTestSuccess { row_count }),
        }
    }

    #[must_use]
    pub fn failure(sql: impl Into<String>, error_message: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            result: Err(QueryTestFailure {
                error_message: error_message.into(),
            }),
        }
    }

    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    #[must_use]
    pub fn passed(&self) -> bool {
        self.result.is_ok()
    }

    #[must_use]
    pub fn row_count(&self) -> Option<u64> {
        self.result.as_ref().ok().map(|success| success.row_count)
    }

    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.result
            .as_ref()
            .err()
            .map(|failure| failure.error_message.as_str())
    }

    pub fn result(&self) -> &std::result::Result<QueryTestSuccess, QueryTestFailure> {
        &self.result
    }
}

/// Structured report for validating one source and its optional test queries.
#[derive(Debug, Clone)]
pub struct SourceValidationReport {
    pub tables: Vec<TableInfo>,
    pub table_functions: Vec<TableFunctionInfo>,
    pub query_tests: Vec<QueryTestResult>,
}

impl SourceValidationReport {
    #[must_use]
    pub fn new(
        tables: Vec<TableInfo>,
        table_functions: Vec<TableFunctionInfo>,
        query_tests: Vec<QueryTestResult>,
    ) -> Self {
        Self {
            tables,
            table_functions,
            query_tests,
        }
    }
}
