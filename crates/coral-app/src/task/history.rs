//! Query-visible read model for retained workspace task history.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use coral_engine::{
    CoreError, RuntimeSystemTable, RuntimeSystemTableColumn, RuntimeSystemTableScan,
};

use crate::state::db::{
    CoralDb, TaskHistoryQuery, TaskHistoryQueryScan, TaskHistoryRelation, TaskHistoryTask,
    TaskHistoryTaskScan,
};
use crate::workspaces::WorkspaceName;

#[derive(Clone)]
pub(crate) struct TaskHistory {
    db: Arc<CoralDb>,
}

impl TaskHistory {
    pub(crate) fn new(db: Arc<CoralDb>) -> Self {
        Self { db }
    }

    pub(crate) fn system_tables(&self, workspace: &WorkspaceName) -> Vec<RuntimeSystemTable> {
        vec![
            self.tasks_table(workspace.clone()),
            self.queries_table(workspace.clone()),
            self.relations_table(workspace.clone()),
        ]
    }

    fn tasks_table(&self, workspace: WorkspaceName) -> RuntimeSystemTable {
        let columns = vec![
            column("task_id", DataType::Utf8, false, "Stable Task identifier."),
            column(
                "intent",
                DataType::Utf8,
                false,
                "Intent declared for the Task.",
            ),
            column(
                "outcome",
                DataType::Utf8,
                true,
                "Terminal Task outcome: success or failure; NULL while active.",
            ),
            column(
                "started_at",
                timestamp_type(),
                false,
                "UTC time when the Task started.",
            ),
            column(
                "completed_at",
                timestamp_type(),
                true,
                "UTC time when the Task completed; NULL while active.",
            ),
            column(
                "query_count",
                DataType::Int64,
                false,
                "Number of retained SQL query rows, including successful and errored queries.",
            ),
        ];
        let schema = schema_for(&columns);
        let db = Arc::clone(&self.db);
        RuntimeSystemTable::new(
            "tasks",
            "Retained workspace Tasks and their SQL query counts.",
            "Task history is retained best-effort activity, not a complete audit log. Filter by task_id when inspecting one Task.",
            columns,
            ["task_id"],
            move |scan| {
                let db = Arc::clone(&db);
                let workspace = workspace.clone();
                let schema = Arc::clone(&schema);
                async move {
                    let rows = db
                        .task_history_state()
                        .tasks(workspace.as_str(), task_scan(&scan))
                        .await
                        .map_err(|error| history_error(&workspace, "tasks", error))?;
                    tasks_batch(schema, &rows)
                }
            },
        )
    }

    fn queries_table(&self, workspace: WorkspaceName) -> RuntimeSystemTable {
        let columns = vec![
            column(
                "query_id",
                DataType::Utf8,
                false,
                "Stable identifier for the retained SQL query.",
            ),
            column(
                "task_id",
                DataType::Utf8,
                false,
                "Task to which the query was attributed.",
            ),
            column(
                "intent",
                DataType::Utf8,
                false,
                "Ordinary SQL tool intent for this query.",
            ),
            column(
                "started_at",
                timestamp_type(),
                false,
                "UTC time when query execution started.",
            ),
            column(
                "sql",
                DataType::Utf8,
                false,
                "SQL text submitted for execution.",
            ),
            column(
                "status",
                DataType::Utf8,
                false,
                "Query result status: success or error.",
            ),
        ];
        let schema = schema_for(&columns);
        let db = Arc::clone(&self.db);
        RuntimeSystemTable::new(
            "task_queries",
            "Retained SQL queries attributed to Tasks in this workspace.",
            "Task history is retained best-effort activity, not a complete audit log. The table has no implicit order; use ORDER BY started_at, query_id for stable display order. This does not represent authored batch order.",
            columns,
            ["task_id", "query_id"],
            move |scan| {
                let db = Arc::clone(&db);
                let workspace = workspace.clone();
                let schema = Arc::clone(&schema);
                async move {
                    let rows = db
                        .task_history_state()
                        .queries(workspace.as_str(), query_scan(&scan))
                        .await
                        .map_err(|error| history_error(&workspace, "task_queries", error))?;
                    queries_batch(schema, &rows)
                }
            },
        )
    }

    fn relations_table(&self, workspace: WorkspaceName) -> RuntimeSystemTable {
        let columns = vec![
            column(
                "query_id",
                DataType::Utf8,
                false,
                "Retained SQL query that used the relation.",
            ),
            column(
                "task_id",
                DataType::Utf8,
                false,
                "Task derived through the retained query relationship.",
            ),
            column(
                "relation_kind",
                DataType::Utf8,
                false,
                "Relation kind: table or table_function.",
            ),
            column(
                "catalog_name",
                DataType::Utf8,
                false,
                "SQL catalog name; empty when the relation has no catalog.",
            ),
            column(
                "schema_name",
                DataType::Utf8,
                false,
                "SQL schema containing the relation.",
            ),
            column(
                "relation_name",
                DataType::Utf8,
                false,
                "Table or table-function name within the schema.",
            ),
        ];
        let schema = schema_for(&columns);
        let db = Arc::clone(&self.db);
        RuntimeSystemTable::new(
            "task_query_relations",
            "Relations used by successful retained Task queries in this workspace.",
            "Task history is retained best-effort activity, not a complete audit log. Failed queries have no relation rows. catalog_name is empty when no catalog exists.",
            columns,
            ["task_id", "query_id"],
            move |scan| {
                let db = Arc::clone(&db);
                let workspace = workspace.clone();
                let schema = Arc::clone(&schema);
                async move {
                    let rows = db
                        .task_history_state()
                        .relations(workspace.as_str(), query_scan(&scan))
                        .await
                        .map_err(|error| {
                            history_error(&workspace, "task_query_relations", error)
                        })?;
                    relations_batch(schema, &rows)
                }
            },
        )
    }
}

fn column(
    name: &str,
    data_type: DataType,
    nullable: bool,
    description: &str,
) -> RuntimeSystemTableColumn {
    RuntimeSystemTableColumn::new(name, data_type, nullable, description)
}

fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
}

fn schema_for(columns: &[RuntimeSystemTableColumn]) -> Arc<Schema> {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(RuntimeSystemTableColumn::field)
            .collect::<Vec<_>>(),
    ))
}

fn task_scan(scan: &RuntimeSystemTableScan) -> TaskHistoryTaskScan<'_> {
    TaskHistoryTaskScan {
        task_id: scan.exact_filter("task_id"),
        limit: scan
            .limit_after_exact_filters()
            .and_then(|limit| u64::try_from(limit).ok()),
    }
}

fn query_scan(scan: &RuntimeSystemTableScan) -> TaskHistoryQueryScan<'_> {
    TaskHistoryQueryScan {
        task_id: scan.exact_filter("task_id"),
        query_id: scan.exact_filter("query_id"),
        limit: scan
            .limit_after_exact_filters()
            .and_then(|limit| u64::try_from(limit).ok()),
    }
}

fn tasks_batch(
    schema: Arc<Schema>,
    rows: &[TaskHistoryTask],
) -> Result<Vec<RecordBatch>, CoreError> {
    let started_at: ArrayRef = Arc::new(
        TimestampNanosecondArray::from(
            rows.iter()
                .map(|row| Some(row.started_at_unix_nanos))
                .collect::<Vec<_>>(),
        )
        .with_timezone("UTC"),
    );
    let completed_at: ArrayRef = Arc::new(
        TimestampNanosecondArray::from(
            rows.iter()
                .map(|row| row.completed_at_unix_nanos)
                .collect::<Vec<_>>(),
        )
        .with_timezone("UTC"),
    );
    batch(
        schema,
        vec![
            strings(rows.iter().map(|row| Some(row.task_id.as_str()))),
            strings(rows.iter().map(|row| Some(row.intent.as_str()))),
            strings(rows.iter().map(|row| row.outcome.as_deref())),
            started_at,
            completed_at,
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|row| row.query_count),
            )),
        ],
    )
}

fn queries_batch(
    schema: Arc<Schema>,
    rows: &[TaskHistoryQuery],
) -> Result<Vec<RecordBatch>, CoreError> {
    let started_at: ArrayRef = Arc::new(
        TimestampNanosecondArray::from(
            rows.iter()
                .map(|row| Some(row.started_at_unix_nanos))
                .collect::<Vec<_>>(),
        )
        .with_timezone("UTC"),
    );
    batch(
        schema,
        vec![
            strings(rows.iter().map(|row| Some(row.query_id.as_str()))),
            strings(rows.iter().map(|row| Some(row.task_id.as_str()))),
            strings(rows.iter().map(|row| Some(row.intent.as_str()))),
            started_at,
            strings(rows.iter().map(|row| Some(row.sql.as_str()))),
            strings(rows.iter().map(|row| Some(row.status.as_str()))),
        ],
    )
}

fn relations_batch(
    schema: Arc<Schema>,
    rows: &[TaskHistoryRelation],
) -> Result<Vec<RecordBatch>, CoreError> {
    batch(
        schema,
        vec![
            strings(rows.iter().map(|row| Some(row.query_id.as_str()))),
            strings(rows.iter().map(|row| Some(row.task_id.as_str()))),
            strings(rows.iter().map(|row| Some(row.relation_kind.as_str()))),
            strings(rows.iter().map(|row| Some(row.catalog_name.as_str()))),
            strings(rows.iter().map(|row| Some(row.schema_name.as_str()))),
            strings(rows.iter().map(|row| Some(row.relation_name.as_str()))),
        ],
    )
}

fn strings<'a>(values: impl IntoIterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(values.into_iter().collect::<StringArray>())
}

fn batch(schema: Arc<Schema>, columns: Vec<ArrayRef>) -> Result<Vec<RecordBatch>, CoreError> {
    RecordBatch::try_new(schema, columns)
        .map(|batch| vec![batch])
        .map_err(CoreError::from)
}

fn history_error(
    workspace: &WorkspaceName,
    table: &str,
    error: impl std::fmt::Display,
) -> CoreError {
    tracing::warn!(workspace = %workspace, table, error = %error, "could not load retained task history");
    CoreError::internal("could not load retained task history")
}
