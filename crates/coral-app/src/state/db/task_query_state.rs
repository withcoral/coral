//! Transactional persistence for workspace-scoped task query activity.

use super::repositories::task_queries::{TaskQueryRelationRow, TaskQueryRow};
use super::{CoralDb, DbError, DbRepos};

pub(crate) struct TaskQueryRelationWrite<'a> {
    pub(crate) relation_kind: &'a str,
    pub(crate) catalog_name: Option<&'a str>,
    pub(crate) schema_name: &'a str,
    pub(crate) relation_name: &'a str,
}

pub(crate) struct TaskQueryWrite<'a> {
    pub(crate) workspace_id: &'a str,
    pub(crate) id: &'a str,
    pub(crate) task_id: &'a str,
    pub(crate) intent: &'a str,
    pub(crate) sql: &'a str,
    pub(crate) status: &'a str,
    pub(crate) started_at_unix_nanos: i64,
    pub(crate) relations: &'a [TaskQueryRelationWrite<'a>],
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskQueryRecord {
    pub(crate) id: String,
    pub(crate) task_id: String,
    pub(crate) intent: String,
    pub(crate) sql: String,
    pub(crate) status: String,
    pub(crate) started_at_unix_nanos: i64,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskQueryRelationRecord {
    pub(crate) query_id: String,
    pub(crate) relation_kind: String,
    pub(crate) catalog_name: Option<String>,
    pub(crate) schema_name: String,
    pub(crate) relation_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskQueryWriteResult {
    Recorded,
    TaskNotFound,
}

pub(crate) struct TaskQueryState<'a> {
    db: &'a CoralDb,
}

impl CoralDb {
    pub(crate) fn task_query_state(&self) -> TaskQueryState<'_> {
        TaskQueryState { db: self }
    }
}

impl TaskQueryState<'_> {
    pub(crate) async fn record(
        &self,
        query: TaskQueryWrite<'_>,
    ) -> Result<TaskQueryWriteResult, DbError> {
        let mut tx = self.db.begin().await?;
        let inserted = tx
            .task_queries()
            .insert_for_workspace(
                query.workspace_id,
                &TaskQueryRow {
                    id: query.id.to_string(),
                    task_id: query.task_id.to_string(),
                    intent: query.intent.to_string(),
                    sql: query.sql.to_string(),
                    status: query.status.to_string(),
                    started_at_unix_nanos: query.started_at_unix_nanos,
                },
            )
            .await?;
        if !inserted {
            tx.rollback().await?;
            return Ok(TaskQueryWriteResult::TaskNotFound);
        }
        let relations = query
            .relations
            .iter()
            .map(|relation| TaskQueryRelationRow {
                query_id: query.id.to_string(),
                relation_kind: relation.relation_kind.to_string(),
                catalog_name: relation.catalog_name.unwrap_or_default().to_string(),
                schema_name: relation.schema_name.to_string(),
                relation_name: relation.relation_name.to_string(),
            })
            .collect::<Vec<_>>();
        tx.task_queries().insert_relations(&relations).await?;
        tx.commit().await?;
        Ok(TaskQueryWriteResult::Recorded)
    }

    #[cfg(test)]
    pub(crate) async fn list_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<TaskQueryRecord>, DbError> {
        let mut session = self.db;
        session
            .task_queries()
            .list_for_workspace(workspace_id)
            .await
            .map(|rows| rows.into_iter().map(TaskQueryRecord::from).collect())
    }

    #[cfg(test)]
    pub(crate) async fn list_relations_for_query(
        &self,
        query_id: &str,
    ) -> Result<Vec<TaskQueryRelationRecord>, DbError> {
        let mut session = self.db;
        session
            .task_queries()
            .list_relations_for_query(query_id)
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(TaskQueryRelationRecord::from)
                    .collect()
            })
    }
}

#[cfg(test)]
impl From<TaskQueryRow> for TaskQueryRecord {
    fn from(row: TaskQueryRow) -> Self {
        Self {
            id: row.id,
            task_id: row.task_id,
            intent: row.intent,
            sql: row.sql,
            status: row.status,
            started_at_unix_nanos: row.started_at_unix_nanos,
        }
    }
}

#[cfg(test)]
impl From<TaskQueryRelationRow> for TaskQueryRelationRecord {
    fn from(row: TaskQueryRelationRow) -> Self {
        Self {
            query_id: row.query_id,
            relation_kind: row.relation_kind,
            catalog_name: (!row.catalog_name.is_empty()).then_some(row.catalog_name),
            schema_name: row.schema_name,
            relation_name: row.relation_name,
        }
    }
}
