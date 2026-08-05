//! Durable SQL activity for validated tasks.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use coral_engine::{CoreError, RuntimeSystemTable};

use crate::state::db::{CoralDb, DbError, DbRepos, TaskQueryRow};
use crate::task::id::TaskId;
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskQueryStatus {
    Success,
    Error,
}

impl TaskQueryStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
        }
    }
}

pub(crate) struct TaskQueryRecord<'a> {
    pub(crate) id: uuid::Uuid,
    pub(crate) task_id: TaskId,
    pub(crate) intent: &'a str,
    pub(crate) sql: &'a str,
    pub(crate) status: TaskQueryStatus,
    pub(crate) started_at_unix_nanos: i64,
}

#[derive(Clone)]
pub(crate) struct TaskActivityRecorder {
    db: Arc<CoralDb>,
}

impl TaskActivityRecorder {
    pub(crate) fn new(db: Arc<CoralDb>) -> Self {
        Self { db }
    }

    pub(crate) async fn record_query(&self, record: TaskQueryRecord<'_>) -> Result<(), DbError> {
        let mut session = self.db.as_ref();
        session
            .task_queries()
            .insert(&TaskQueryRow {
                id: record.id.to_string(),
                task_id: record.task_id.to_string(),
                intent: record.intent.to_string(),
                sql: record.sql.to_string(),
                status: record.status.as_str().to_string(),
                started_at_unix_nanos: record.started_at_unix_nanos,
            })
            .await
    }

    pub(crate) async fn queries_for_workspace(
        &self,
        workspace: &WorkspaceName,
    ) -> Result<Vec<TaskQueryRow>, DbError> {
        let mut session = self.db.as_ref();
        session
            .task_queries()
            .list_for_workspace(workspace.as_str())
            .await
    }

    pub(crate) fn system_table(&self, workspace: WorkspaceName) -> RuntimeSystemTable {
        let schema = Arc::new(Schema::new(vec![
            Field::new("query_id", DataType::Utf8, false),
            Field::new("task_id", DataType::Utf8, false),
            Field::new("intent", DataType::Utf8, false),
            Field::new(
                "executed_at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("sql", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
        ]));
        let recorder = self.clone();
        let loader_schema = Arc::clone(&schema);
        RuntimeSystemTable::new(
            "task_queries",
            "Retained SQL statements attributed to tasks in this workspace.",
            "Filter by task_id and order by executed_at, query_id for deterministic execution-start order.",
            schema,
            move || {
                let recorder = recorder.clone();
                let workspace = workspace.clone();
                let schema = Arc::clone(&loader_schema);
                async move {
                    let rows =
                        recorder
                            .queries_for_workspace(&workspace)
                            .await
                            .map_err(|error| {
                                tracing::warn!(
                                    workspace = %workspace,
                                    error = %error,
                                    "could not load task query activity"
                                );
                                CoreError::internal("could not load task query activity")
                            })?;
                    let executed_at: ArrayRef = Arc::new(
                        TimestampNanosecondArray::from(
                            rows.iter()
                                .map(|row| Some(row.started_at_unix_nanos))
                                .collect::<Vec<_>>(),
                        )
                        .with_timezone("UTC"),
                    );
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(
                                rows.iter()
                                    .map(|row| Some(row.id.as_str()))
                                    .collect::<StringArray>(),
                            ),
                            Arc::new(
                                rows.iter()
                                    .map(|row| Some(row.task_id.as_str()))
                                    .collect::<StringArray>(),
                            ),
                            Arc::new(
                                rows.iter()
                                    .map(|row| Some(row.intent.as_str()))
                                    .collect::<StringArray>(),
                            ),
                            executed_at,
                            Arc::new(
                                rows.iter()
                                    .map(|row| Some(row.sql.as_str()))
                                    .collect::<StringArray>(),
                            ),
                            Arc::new(
                                rows.iter()
                                    .map(|row| Some(row.status.as_str()))
                                    .collect::<StringArray>(),
                            ),
                        ],
                    )
                    .map_err(CoreError::from)?;
                    Ok(vec![batch])
                }
            },
        )
    }
}
