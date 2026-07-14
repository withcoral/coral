//! Exact-match trajectory capture, distillation, indexing, and retrieval.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::bootstrap::AppError;
use crate::state::db::{
    ConsolidatedPathRecord, CoralDb, DbRepos, DistillationRecord, DistilledStepRecord,
    ExactIndexRecord, IndexBuildRecord, PathCandidateRecord, RawTrajectoryStepRecord,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SuggestedPathStep {
    pub(crate) sql_template: String,
    pub(crate) relations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SuggestedPath {
    pub(crate) path_id: String,
    pub(crate) support_count: u64,
    pub(crate) relations: Vec<String>,
    pub(crate) steps: Vec<SuggestedPathStep>,
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

    pub(crate) async fn distill_and_index(
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
        let output_step_count = usize_to_i64(distilled_steps.len(), "distilled step count")?;
        let distillation = DistillationRecord {
            id: distillation_id.clone(),
            task_id,
            strategy: DISTILLATION_STRATEGY.to_string(),
            normalized_intent: normalize_intent(&task.intent),
            path_key: path_key(&distilled_steps),
            input_step_count: usize_to_i64(raw_steps.len(), "raw trajectory step count")?,
            output_step_count,
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

        let normalized_intent = &distillation.normalized_intent;
        let candidates = tx
            .trajectory_memory()
            .list_path_candidates_for_intent(workspace.as_str(), normalized_intent)
            .await?;
        let groups = consolidate_candidates(candidates);
        let winner = groups.first();
        tx.trajectory_memory()
            .delete_consolidated_paths_for_intent(workspace.as_str(), normalized_intent)
            .await?;
        for group in &groups {
            tx.trajectory_memory()
                .insert_consolidated_path(
                    workspace.as_str(),
                    &ConsolidatedPathRecord {
                        normalized_intent: distillation.normalized_intent.clone(),
                        path_key: group.representative.path_key.clone(),
                        representative_distillation_id: group
                            .representative
                            .distillation_id
                            .clone(),
                        support_count: group.support_count,
                        step_count: group.representative.step_count,
                        updated_at_unix_nanos: created_at_unix_nanos,
                    },
                )
                .await?;
        }
        if let Some(winner) = winner {
            tx.trajectory_memory()
                .upsert_exact_index(
                    workspace.as_str(),
                    &ExactIndexRecord {
                        normalized_intent: distillation.normalized_intent.clone(),
                        path_key: winner.representative.path_key.clone(),
                        support_count: winner.support_count,
                        updated_at_unix_nanos: created_at_unix_nanos,
                    },
                )
                .await?;
        }
        tx.trajectory_memory()
            .insert_index_build(
                workspace.as_str(),
                &IndexBuildRecord {
                    id: format!("build_{}", uuid::Uuid::new_v4().simple()),
                    normalized_intent: distillation.normalized_intent,
                    candidate_path_count: usize_to_i64(groups.len(), "candidate path count")?,
                    selected_distillation_id: winner
                        .map(|group| group.representative.distillation_id.clone()),
                    selected_path_key: winner.map(|group| group.representative.path_key.clone()),
                    selected_support_count: winner.map_or(0, |group| group.support_count),
                    created_at_unix_nanos,
                },
            )
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn suggested_paths(
        &self,
        workspace: &WorkspaceName,
        intent: &str,
    ) -> Result<Vec<SuggestedPath>, AppError> {
        let normalized_intent = normalize_intent(intent);
        let mut session = self.db.as_ref();
        let Some(index) = session
            .trajectory_memory()
            .get_exact_index(workspace.as_str(), &normalized_intent)
            .await?
        else {
            return Ok(Vec::new());
        };
        let Some(consolidated) = session
            .trajectory_memory()
            .get_consolidated_path(workspace.as_str(), &normalized_intent, &index.path_key)
            .await?
        else {
            return Ok(Vec::new());
        };
        if index.support_count != consolidated.support_count {
            return Err(AppError::FailedPrecondition(format!(
                "trajectory index support {} does not match consolidated support {}",
                index.support_count, consolidated.support_count
            )));
        }
        let records = session
            .trajectory_memory()
            .list_distilled_steps(
                workspace.as_str(),
                &consolidated.representative_distillation_id,
            )
            .await?;
        let mut all_relations = BTreeSet::new();
        let mut steps = Vec::with_capacity(records.len());
        for record in records {
            let relations = parse_relations(&record.relations_json)?;
            all_relations.extend(relations.iter().cloned());
            steps.push(SuggestedPathStep {
                sql_template: record.sql_template,
                relations,
            });
        }
        let support_count = u64::try_from(consolidated.support_count).map_err(|error| {
            AppError::FailedPrecondition(format!("trajectory support count is invalid: {error}"))
        })?;
        Ok(vec![SuggestedPath {
            path_id: index.path_key,
            support_count,
            relations: all_relations.into_iter().collect(),
            steps,
        }])
    }
}

#[derive(Debug)]
struct CandidateGroup {
    representative: PathCandidateRecord,
    support_count: i64,
}

fn consolidate_candidates(candidates: Vec<PathCandidateRecord>) -> Vec<CandidateGroup> {
    let mut grouped = BTreeMap::<String, CandidateGroup>::new();
    for candidate in candidates {
        let key = candidate.path_key.clone();
        let group = grouped.entry(key).or_insert_with(|| CandidateGroup {
            representative: candidate.clone(),
            support_count: 0,
        });
        group.support_count = group.support_count.saturating_add(1);
        if candidate.created_at_unix_nanos > group.representative.created_at_unix_nanos {
            group.representative = candidate;
        }
    }
    let mut groups = grouped.into_values().collect::<Vec<_>>();
    groups.sort_by(|left, right| {
        right
            .support_count
            .cmp(&left.support_count)
            .then_with(|| {
                left.representative
                    .step_count
                    .cmp(&right.representative.step_count)
            })
            .then_with(|| {
                left.representative
                    .path_key
                    .cmp(&right.representative.path_key)
            })
    });
    groups
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

fn parse_relations(raw: &str) -> Result<Vec<String>, AppError> {
    serde_json::from_str(raw).map_err(Into::into)
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
        PathCandidateRecord, RawTrajectoryStep, TrajectoryMemoryManager, TrajectoryOutputSummary,
        consolidate_candidates, parameterize_sql,
    };
    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::{CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig};
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

    #[test]
    fn consolidation_prefers_support_then_shortness() {
        let candidates = vec![
            candidate("long", "task-a", 3, 1),
            candidate("long", "task-b", 3, 2),
            candidate("short", "task-c", 1, 3),
            candidate("short", "task-d", 1, 4),
        ];
        let groups = consolidate_candidates(candidates);
        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups.first().expect("winner").representative.path_key,
            "short"
        );
        assert_eq!(groups.first().expect("winner").support_count, 2);
    }

    #[tokio::test]
    async fn exact_match_retrieval_is_cold_then_warm_against_sqlite() {
        let (temp, db) = open_sqlite().await;

        assert_exact_match_retrieval(db, WorkspaceName::default()).await;

        drop(temp);
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared trajectory harness against Postgres"]
    async fn exact_match_retrieval_is_cold_then_warm_against_postgres() {
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

        assert_exact_match_retrieval(db, workspace).await;
    }

    #[expect(
        clippy::too_many_lines,
        reason = "The shared backend scenario keeps the cold-to-warm lifecycle in one readable test flow."
    )]
    async fn assert_exact_match_retrieval(db: Arc<CoralDb>, workspace: WorkspaceName) {
        let memory = TrajectoryMemoryManager::new(Arc::clone(&db));
        let tasks = TaskManager::new(TaskStore::new(Arc::clone(&db)), memory.clone());
        let intent = "Find renewal risk";

        let cold = tasks
            .start_task(workspace.clone(), intent.to_string())
            .await
            .expect("start cold task");
        assert!(cold.suggested_paths.is_empty());

        for (started_at_unix_nanos, status) in [(10, "success"), (20, "success")] {
            memory
                .record_raw_step(
                    &workspace,
                    RawTrajectoryStep {
                        task_id: cold.id,
                        started_at_unix_nanos,
                        completed_at_unix_nanos: started_at_unix_nanos + 1,
                        operation: "execute_sql".to_string(),
                        input: "SELECT customer_id FROM crm.accounts WHERE region = 'emea' AND score > 42 -- live values".to_string(),
                        status,
                        row_count: Some(3),
                        output_summary: Some(TrajectoryOutputSummary {
                            sources: vec!["crm".to_string()],
                            relations: vec!["crm.accounts".to_string()],
                            column_count: Some(1),
                        }),
                        error_kind: None,
                        error_type: None,
                        error_message: None,
                    },
                )
                .await
                .expect("record successful raw step");
        }
        memory
            .record_raw_step(
                &workspace,
                RawTrajectoryStep {
                    task_id: cold.id,
                    started_at_unix_nanos: 30,
                    completed_at_unix_nanos: 31,
                    operation: "execute_sql".to_string(),
                    input: "SELECT * FROM missing.table".to_string(),
                    status: "error",
                    row_count: None,
                    output_summary: None,
                    error_kind: Some("engine".to_string()),
                    error_type: Some("EXECUTION".to_string()),
                    error_message: Some("table not found".to_string()),
                },
            )
            .await
            .expect("record failed raw step");

        tasks
            .end_task(workspace.clone(), cold.id, TaskStatus::Success)
            .await
            .expect("end cold task");

        let warm = tasks
            .start_task(workspace.clone(), "  FIND   renewal RISK  ".to_string())
            .await
            .expect("start warm task");
        let suggested = warm.suggested_paths.first().expect("suggested path");
        assert_eq!(warm.suggested_paths.len(), 1);
        assert_eq!(suggested.support_count, 1);
        assert_eq!(suggested.relations, ["crm.accounts"]);
        assert_eq!(
            suggested.steps.len(),
            1,
            "duplicate SQL should be distilled once"
        );
        let suggested_step = suggested.steps.first().expect("suggested step");
        assert_eq!(
            suggested_step.sql_template,
            "SELECT customer_id FROM crm.accounts WHERE region = :param_1 AND score > :param_2"
        );
        assert_eq!(suggested_step.relations, ["crm.accounts"]);
        assert!(
            memory
                .suggested_paths(&workspace, "Find renewal risk!")
                .await
                .expect("lookup punctuation miss")
                .is_empty(),
            "exact matching preserves punctuation"
        );

        let mut session = db.as_ref();
        let raw = session
            .trajectory_memory()
            .list_raw_steps_for_task(workspace.as_str(), &cold.id.to_string())
            .await
            .expect("list raw steps");
        assert_eq!(raw.len(), 3, "every attributed operation should persist");
        let index = session
            .trajectory_memory()
            .get_exact_index(workspace.as_str(), "find renewal risk")
            .await
            .expect("get exact index")
            .expect("exact index");
        let consolidated = session
            .trajectory_memory()
            .get_consolidated_path(workspace.as_str(), "find renewal risk", &index.path_key)
            .await
            .expect("get consolidated path")
            .expect("consolidated path");
        assert_eq!(consolidated.support_count, 1);
        assert_eq!(consolidated.step_count, 1);
        let distillation = session
            .trajectory_memory()
            .get_distillation(
                workspace.as_str(),
                &consolidated.representative_distillation_id,
            )
            .await
            .expect("get distillation")
            .expect("distillation");
        assert_eq!(distillation.input_step_count, 3);
        assert_eq!(distillation.output_step_count, 1);
        assert_eq!(distillation.normalized_intent, "find renewal risk");
        let distilled_steps = session
            .trajectory_memory()
            .list_distilled_steps(
                workspace.as_str(),
                &consolidated.representative_distillation_id,
            )
            .await
            .expect("list distilled steps");
        let distilled_step = distilled_steps.first().expect("distilled step");
        assert_eq!(distilled_step.result_row_count, Some(3));
        assert_eq!(distilled_step.result_column_count, Some(1));
    }

    async fn open_sqlite() -> (TempDir, Arc<CoralDb>) {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("default test config should be sqlite");
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

    fn candidate(
        path_key: &str,
        task_id: &str,
        step_count: i64,
        created_at_unix_nanos: i64,
    ) -> PathCandidateRecord {
        PathCandidateRecord {
            distillation_id: format!("dist-{task_id}"),
            path_key: path_key.to_string(),
            step_count,
            created_at_unix_nanos,
        }
    }
}
