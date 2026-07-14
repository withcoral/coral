//! Raw task-attributed trajectory capture and deterministic SQL distillation.

use std::collections::BTreeSet;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::bootstrap::AppError;
use crate::state::db::{
    CoralDb, DbRepos, DistillationRecord, DistilledStepRecord, RawTrajectoryStepRecord,
    now_unix_nanos_i64,
};
use crate::task::id::TaskId;
use crate::workspaces::WorkspaceName;

const DISTILLATION_STRATEGY: &str = "oracle_free_successful_sql_v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct TrajectoryOutputSummary {
    pub(crate) sources: Vec<String>,
    pub(crate) relations: Vec<String>,
    #[serde(default)]
    pub(crate) column_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawTrajectoryStep {
    pub(crate) task_id: TaskId,
    pub(crate) started_at_unix_nanos: i64,
    pub(crate) completed_at_unix_nanos: i64,
    pub(crate) operation: String,
    pub(crate) input: String,
    pub(crate) status: &'static str,
    pub(crate) row_count: Option<u64>,
    pub(crate) output_summary: Option<TrajectoryOutputSummary>,
    pub(crate) error_kind: Option<String>,
    pub(crate) error_type: Option<String>,
    pub(crate) error_message: Option<String>,
}

#[derive(Clone)]
pub(crate) struct TrajectoryMemoryManager {
    db: Arc<CoralDb>,
}

impl TrajectoryMemoryManager {
    pub(crate) fn new(db: Arc<CoralDb>) -> Self {
        Self { db }
    }

    pub(crate) async fn record_raw_step(
        &self,
        workspace: &WorkspaceName,
        step: RawTrajectoryStep,
    ) -> Result<(), AppError> {
        let task_id = step.task_id.to_string();
        let mut session = self.db.as_ref();
        if session
            .tasks()
            .get(workspace.as_str(), &task_id)
            .await?
            .is_none()
        {
            return Ok(());
        }
        let row_count = step
            .row_count
            .map(i64::try_from)
            .transpose()
            .map_err(|error| {
                AppError::FailedPrecondition(format!("trajectory row count exceeds i64: {error}"))
            })?;
        let output_summary_json = step
            .output_summary
            .map(|summary| serde_json::to_string(&summary))
            .transpose()?;
        session
            .trajectory_memory()
            .insert_raw_step(
                workspace.as_str(),
                &RawTrajectoryStepRecord {
                    id: format!("raw_{}", uuid::Uuid::new_v4().simple()),
                    task_id,
                    started_at_unix_nanos: step.started_at_unix_nanos,
                    completed_at_unix_nanos: step.completed_at_unix_nanos,
                    operation: step.operation,
                    input: step.input,
                    status: step.status.to_string(),
                    row_count,
                    output_summary_json,
                    error_kind: step.error_kind,
                    error_type: step.error_type,
                    error_message: step.error_message,
                },
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn distill_task(
        &self,
        workspace: &WorkspaceName,
        task_id: &TaskId,
    ) -> Result<(), AppError> {
        let task_id = task_id.to_string();
        let mut session = self.db.as_ref();
        let task = session
            .tasks()
            .get(workspace.as_str(), &task_id)
            .await?
            .ok_or_else(|| {
                AppError::FailedPrecondition(format!("task '{task_id}' was not found"))
            })?;
        let raw_steps = session
            .trajectory_memory()
            .list_raw_steps_for_task(workspace.as_str(), &task_id)
            .await?;
        let created_at_unix_nanos = now_unix_nanos_i64()?;
        let distillation_id = stable_id("dist", [task_id.as_str()]);
        let distilled_steps = distill_steps(&distillation_id, &raw_steps, created_at_unix_nanos)?;
        let distillation = DistillationRecord {
            id: distillation_id.clone(),
            task_id,
            strategy: DISTILLATION_STRATEGY.to_string(),
            normalized_intent: normalize_intent(&task.intent),
            path_key: path_key(&distilled_steps),
            input_step_count: usize_to_i64(raw_steps.len(), "raw trajectory step count")?,
            output_step_count: usize_to_i64(
                distilled_steps.len(),
                "distilled trajectory step count",
            )?,
            created_at_unix_nanos,
        };

        let mut tx = self.db.begin().await?;
        tx.trajectory_memory()
            .delete_distilled_steps(workspace.as_str(), &distillation_id)
            .await?;
        tx.trajectory_memory()
            .upsert_distillation(workspace.as_str(), &distillation)
            .await?;
        for step in &distilled_steps {
            tx.trajectory_memory()
                .insert_distilled_step(workspace.as_str(), step)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

fn distill_steps(
    distillation_id: &str,
    raw_steps: &[RawTrajectoryStepRecord],
    created_at_unix_nanos: i64,
) -> Result<Vec<DistilledStepRecord>, AppError> {
    let mut seen = BTreeSet::new();
    let mut distilled = Vec::new();
    for raw in raw_steps {
        if raw.operation != "execute_sql" || raw.status != "success" {
            continue;
        }
        let sql_template = parameterize_sql(&raw.input);
        if sql_template.is_empty() {
            continue;
        }
        let summary = raw
            .output_summary_json
            .as_deref()
            .map(serde_json::from_str::<TrajectoryOutputSummary>)
            .transpose()?
            .unwrap_or_default();
        let result_column_count = summary
            .column_count
            .map(i64::try_from)
            .transpose()
            .map_err(|error| {
                AppError::FailedPrecondition(format!(
                    "trajectory column count exceeds i64: {error}"
                ))
            })?;
        let relations = summary.relations.into_iter().collect::<BTreeSet<_>>();
        let relations_json = serde_json::to_string(&relations.iter().collect::<Vec<_>>())?;
        let exact_key = stable_id("step", [sql_template.as_str()]);
        if !seen.insert(exact_key.clone()) {
            continue;
        }
        let ordinal = usize_to_i64(distilled.len(), "distilled step ordinal")?;
        let ordinal_string = ordinal.to_string();
        distilled.push(DistilledStepRecord {
            id: stable_id(
                "dstep",
                [distillation_id, ordinal_string.as_str(), exact_key.as_str()],
            ),
            distillation_id: distillation_id.to_string(),
            source_raw_step_id: raw.id.clone(),
            ordinal,
            sql_template,
            relations_json,
            result_row_count: raw.row_count,
            result_column_count,
            exact_key,
            created_at_unix_nanos,
        });
    }
    Ok(distilled)
}

fn path_key(steps: &[DistilledStepRecord]) -> String {
    stable_id("path", steps.iter().map(|step| step.exact_key.as_str()))
}

fn normalize_intent(intent: &str) -> String {
    intent
        .split_whitespace()
        .map(str::to_lowercase)
        .collect::<Vec<_>>()
        .join(" ")
}

fn parameterize_sql(sql: &str) -> String {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, trimmed).tokenize() else {
        return collapse_sql_whitespace(trimmed);
    };
    let mut parameter_index = 0_u64;
    let mut template = String::new();
    for token in tokens {
        match token {
            Token::EOF => {}
            Token::Whitespace(_) => push_space(&mut template),
            token if is_literal(&token) => {
                parameter_index = parameter_index.saturating_add(1);
                template.push_str(":param_");
                template.push_str(&parameter_index.to_string());
            }
            token => template.push_str(&token.to_string()),
        }
    }
    collapse_sql_whitespace(&template)
}

fn is_literal(token: &Token) -> bool {
    match token {
        Token::Number(value, _) => !matches!(value.as_str(), "0" | "1" | "100"),
        Token::SingleQuotedString(_)
        | Token::TripleSingleQuotedString(_)
        | Token::DollarQuotedString(_)
        | Token::SingleQuotedByteStringLiteral(_)
        | Token::TripleSingleQuotedByteStringLiteral(_)
        | Token::SingleQuotedRawStringLiteral(_)
        | Token::TripleSingleQuotedRawStringLiteral(_)
        | Token::NationalStringLiteral(_)
        | Token::EscapedStringLiteral(_)
        | Token::UnicodeStringLiteral(_)
        | Token::HexStringLiteral(_) => true,
        _ => false,
    }
}

fn push_space(sql: &mut String) {
    if !sql.is_empty() && !sql.ends_with(' ') {
        sql.push(' ');
    }
}

fn collapse_sql_whitespace(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn stable_id<'a>(prefix: &str, parts: impl IntoIterator<Item = &'a str>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(prefix.as_bytes());
    for part in parts {
        hasher.update([0]);
        hasher.update(part.as_bytes());
    }
    format!("{prefix}_{:x}", hasher.finalize())
}

fn usize_to_i64(value: usize, name: &str) -> Result<i64, AppError> {
    i64::try_from(value)
        .map_err(|error| AppError::FailedPrecondition(format!("{name} exceeds i64: {error}")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::{
        RawTrajectoryStep, TrajectoryMemoryManager, TrajectoryOutputSummary, parameterize_sql,
        stable_id,
    };
    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::{CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig};
    use crate::task::id::TaskId;
    use crate::task::manager::TaskManager;
    use crate::task::store::{TaskStatus, TaskStore};
    use crate::workspaces::WorkspaceName;

    #[test]
    fn parameterizes_literals_and_removes_comments() {
        assert_eq!(
            parameterize_sql(
                "SELECT * FROM orders WHERE customer = 'Daisey' AND id = 42 -- private"
            ),
            "SELECT * FROM orders WHERE customer = :param_1 AND id = :param_2"
        );
        assert_eq!(
            parameterize_sql("SELECT 0, 1, 100, 101"),
            "SELECT 0, 1, 100, :param_1"
        );
    }

    #[tokio::test]
    async fn successful_sql_distills_against_sqlite() {
        let (temp, db) = open_sqlite().await;

        assert_successful_sql_distills(db, WorkspaceName::default()).await;

        drop(temp);
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared trajectory harness against Postgres"]
    async fn successful_sql_distills_against_postgres() {
        let Some(url) = postgres_test_url() else {
            return;
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
                .await
                .expect("open postgres"),
        );
        db.migrate().await.expect("migrate postgres");
        let workspace =
            WorkspaceName::parse(&format!("trajectory_{}", uuid::Uuid::new_v4().simple()))
                .expect("workspace");

        assert_successful_sql_distills(db, workspace).await;
    }

    async fn assert_successful_sql_distills(db: Arc<CoralDb>, workspace: WorkspaceName) {
        let memory = TrajectoryMemoryManager::new(Arc::clone(&db));
        let tasks = TaskManager::new(TaskStore::new(Arc::clone(&db)), memory.clone());
        let task = tasks
            .start_task(workspace.clone(), "  Find   Renewal RISK  ".to_string())
            .await
            .expect("start task");

        memory
            .record_raw_step(&workspace, successful_sql_step(task.id, 20, "emea", 3))
            .await
            .expect("record successful step");
        memory
            .record_raw_step(
                &workspace,
                RawTrajectoryStep {
                    task_id: task.id,
                    started_at_unix_nanos: 10,
                    completed_at_unix_nanos: 11,
                    operation: "search".to_string(),
                    input: "renewal risk".to_string(),
                    status: "error",
                    row_count: None,
                    output_summary: None,
                    error_kind: Some("app".to_string()),
                    error_type: Some("SEARCH".to_string()),
                    error_message: Some("search failed".to_string()),
                },
            )
            .await
            .expect("record failed step");
        memory
            .record_raw_step(&workspace, successful_sql_step(task.id, 22, "apac", 2))
            .await
            .expect("record duplicate successful step");
        memory
            .record_raw_step(
                &workspace,
                RawTrajectoryStep {
                    task_id: TaskId::new(),
                    started_at_unix_nanos: 30,
                    completed_at_unix_nanos: 31,
                    operation: "execute_sql".to_string(),
                    input: "SELECT 1".to_string(),
                    status: "success",
                    row_count: Some(1),
                    output_summary: None,
                    error_kind: None,
                    error_type: None,
                    error_message: None,
                },
            )
            .await
            .expect("unknown task is ignored");

        let mut session = db.as_ref();
        let raw = session
            .trajectory_memory()
            .list_raw_steps_for_task(workspace.as_str(), &task.id.to_string())
            .await
            .expect("list raw steps");
        assert_raw_steps(&raw);

        tasks
            .end_task(workspace.clone(), task.id, TaskStatus::Success)
            .await
            .expect("end task");

        let distillation_id = stable_id("dist", [task.id.to_string().as_str()]);
        let distillation = session
            .trajectory_memory()
            .get_distillation(workspace.as_str(), &distillation_id)
            .await
            .expect("get distillation")
            .expect("distillation");
        assert_eq!(distillation.input_step_count, 3);
        assert_eq!(distillation.output_step_count, 1);
        assert_eq!(distillation.normalized_intent, "find renewal risk");
        let distilled = session
            .trajectory_memory()
            .list_distilled_steps(workspace.as_str(), &distillation_id)
            .await
            .expect("list distilled steps");
        let [step] = distilled.as_slice() else {
            panic!("expected one deduplicated distilled step: {distilled:#?}");
        };
        assert_eq!(
            step.sql_template,
            "SELECT customer_id FROM crm.accounts WHERE region = :param_1"
        );
        assert_eq!(step.result_row_count, Some(3));
        assert_eq!(step.result_column_count, Some(1));
        assert_eq!(step.relations_json, r#"["crm.accounts"]"#);
    }

    fn successful_sql_step(
        task_id: TaskId,
        started_at_unix_nanos: i64,
        region: &str,
        row_count: u64,
    ) -> RawTrajectoryStep {
        RawTrajectoryStep {
            task_id,
            started_at_unix_nanos,
            completed_at_unix_nanos: started_at_unix_nanos + 1,
            operation: "execute_sql".to_string(),
            input: format!("SELECT customer_id FROM crm.accounts WHERE region = '{region}'"),
            status: "success",
            row_count: Some(row_count),
            output_summary: Some(TrajectoryOutputSummary {
                sources: vec!["crm".to_string()],
                relations: vec!["crm.accounts".to_string()],
                column_count: Some(1),
            }),
            error_kind: None,
            error_type: None,
            error_message: None,
        }
    }

    fn assert_raw_steps(raw: &[crate::state::db::RawTrajectoryStepRecord]) {
        let [search, sql, duplicate] = raw else {
            panic!("expected three raw steps: {raw:#?}");
        };
        assert_eq!(search.operation, "search");
        assert_eq!(search.status, "error");
        assert_eq!(search.error_type.as_deref(), Some("SEARCH"));
        assert_eq!(sql.operation, "execute_sql");
        assert_eq!(sql.row_count, Some(3));
        let summary = sql.output_summary_json.as_deref().expect("output summary");
        assert!(summary.contains("crm.accounts"));
        assert!(!summary.contains("customer_id"));
        assert!(duplicate.input.contains("apac"));
    }

    async fn open_sqlite() -> (TempDir, Arc<CoralDb>) {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("default database is sqlite");
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate sqlite");
        (temp, db)
    }

    fn postgres_test_url() -> Option<String> {
        bootstrap::env_var("CORAL_TEST_POSTGRES_URL").expect("read CORAL_TEST_POSTGRES_URL")
    }
}
