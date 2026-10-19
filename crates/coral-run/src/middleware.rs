//! Durable Middleware execution evidence for one claimed Run.
//!
//! Each Middleware execution commits one record and a `middleware.started`
//! Event before its enter phase runs, flips to its exit phase when its
//! continuation settles, and commits its final value preview or failure with
//! `middleware.ended`. Evidence follows the Run's claim and visibility rules.

use std::sync::Arc;

use coral_workspace::WorkspaceId;

use crate::engine::Clock;
use crate::run_repository::NewEvent;
use crate::{Error, RunId, middleware_repository, run_repository};

/// One Middleware the Run pinned at admission.
#[derive(Debug, Clone)]
pub(crate) struct PinnedMiddleware {
    pub(crate) position: usize,
    pub(crate) id: String,
    pub(crate) name: String,
}

/// Invocation level wrapped by one Middleware execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiddlewareScope {
    /// The Program Run.
    Run,
    /// One Saved Function call.
    Function,
    /// One Operation Invocation.
    Operation,
}

impl MiddlewareScope {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Function => "function",
            Self::Operation => "operation",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "run" => Ok(Self::Run),
            "function" => Ok(Self::Function),
            "operation" => Ok(Self::Operation),
            _ => Err(Error::CorruptData("invalid Middleware scope".into())),
        }
    }
}

impl From<coral_v8::MiddlewareScope> for MiddlewareScope {
    fn from(scope: coral_v8::MiddlewareScope) -> Self {
        match scope {
            coral_v8::MiddlewareScope::Run => Self::Run,
            coral_v8::MiddlewareScope::Function => Self::Function,
            coral_v8::MiddlewareScope::Operation => Self::Operation,
        }
    }
}

/// Which side of `next()` one Middleware execution has reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiddlewarePhase {
    /// Before its continuation settled, including when it never calls it.
    Enter,
    /// After its continuation settled.
    Exit,
}

impl MiddlewarePhase {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Enter => "enter",
            Self::Exit => "exit",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "enter" => Ok(Self::Enter),
            "exit" => Ok(Self::Exit),
            _ => Err(Error::CorruptData("invalid Middleware phase".into())),
        }
    }
}

/// State of one Middleware execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiddlewareExecutionStatus {
    /// Still running.
    Running,
    /// Returned a value.
    Succeeded,
    /// Threw, rejected, or finished before its continuation settled.
    Failed,
    /// The Run ended while it was running.
    RunTerminated,
}

impl MiddlewareExecutionStatus {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::RunTerminated => "run_terminated",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "run_terminated" => Ok(Self::RunTerminated),
            _ => Err(Error::CorruptData("invalid Middleware status".into())),
        }
    }
}

/// One Middleware execution as Activity shows it: a single row with its phase
/// and status. It follows the enclosing Run's visibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MiddlewareExecutionSummary {
    /// Stable execution identity within the Run.
    pub id: String,
    /// Enclosing Run.
    pub run: RunId,
    /// Pinned chain position, outermost first.
    pub position: usize,
    /// Pinned Middleware identity.
    pub middleware_id: String,
    /// Pinned Middleware name.
    pub name: String,
    /// Wrapped invocation level.
    pub scope: MiddlewareScope,
    /// Wrapped Call Path; absent for the Run.
    pub target: Option<String>,
    /// Enter or exit phase.
    pub phase: MiddlewarePhase,
    /// Current status.
    pub status: MiddlewareExecutionStatus,
    /// When the execution started.
    pub started_at: i64,
    /// When it ended.
    pub ended_at: Option<i64>,
    /// Elapsed time so far.
    pub duration: i64,
    /// Bounded non-executing preview of the returned value, as JSON.
    pub preview_json: Option<String>,
    /// Bounded summary of the failure.
    pub failure_summary: Option<String>,
}

#[derive(Clone)]
pub(crate) struct MiddlewareRecorder {
    database: Arc<coral_db::Database>,
    run_id: RunId,
    workspace_id: WorkspaceId,
    claim_id: String,
    clock: Arc<dyn Clock>,
    chain: Arc<Vec<PinnedMiddleware>>,
}

impl MiddlewareRecorder {
    pub(crate) fn new(
        database: Arc<coral_db::Database>,
        run_id: RunId,
        workspace_id: WorkspaceId,
        claim_id: String,
        clock: Arc<dyn Clock>,
        chain: Vec<PinnedMiddleware>,
    ) -> Self {
        Self {
            database,
            run_id,
            workspace_id,
            claim_id,
            clock,
            chain: Arc::new(chain),
        }
    }

    pub(crate) async fn started(
        &self,
        start: &coral_v8::MiddlewareExecutionStart,
    ) -> Result<String, Error> {
        let middleware = self
            .chain
            .iter()
            .find(|middleware| middleware.position == start.position)
            .ok_or_else(|| Error::CorruptData("Middleware is not in the pinned chain".into()))?;
        let id = uuid::Uuid::new_v4().to_string();
        let mut transaction = self.database.begin().await?;
        let (run, now) = self.current(&mut transaction).await?;
        middleware_repository::insert_execution(
            &mut transaction,
            middleware_repository::NewExecution {
                id: &id,
                workspace_id: &self.workspace_id,
                run_id: &self.run_id,
                middleware,
                scope: start.scope.into(),
                target: start.target.as_deref(),
                started_at_unix_nanos: now,
            },
        )
        .await?;
        run_repository::advance_event_sequence(&mut transaction, &run, &self.claim_id, 1, 0)
            .await?;
        run_repository::insert_event(
            &mut transaction,
            &self.workspace_id,
            &self.run_id,
            run.next_event_sequence,
            NewEvent::MiddlewareStarted(&id),
            now,
        )
        .await?;
        transaction.commit().await?;
        Ok(id)
    }

    pub(crate) async fn continued(&self, id: &str) -> Result<(), Error> {
        let mut transaction = self.database.begin().await?;
        self.current(&mut transaction).await?;
        if !middleware_repository::mark_exit(&mut transaction, &self.workspace_id, &self.run_id, id)
            .await?
        {
            return Err(Error::RunEventPersistence(
                "Middleware execution is not running".into(),
            ));
        }
        transaction.commit().await?;
        Ok(())
    }

    pub(crate) async fn ended(
        &self,
        id: &str,
        outcome: &coral_v8::MiddlewareExecutionOutcome,
    ) -> Result<(), Error> {
        let mut transaction = self.database.begin().await?;
        let (run, now) = self.current(&mut transaction).await?;
        if !middleware_repository::finish_execution(
            &mut transaction,
            &self.workspace_id,
            &self.run_id,
            id,
            outcome,
            now,
        )
        .await?
        {
            return Err(Error::RunEventPersistence(
                "Middleware execution is not running".into(),
            ));
        }
        let status = match outcome {
            coral_v8::MiddlewareExecutionOutcome::Succeeded { .. } => {
                MiddlewareExecutionStatus::Succeeded
            }
            coral_v8::MiddlewareExecutionOutcome::Failed { .. } => {
                MiddlewareExecutionStatus::Failed
            }
        };
        run_repository::advance_event_sequence(&mut transaction, &run, &self.claim_id, 1, 0)
            .await?;
        run_repository::insert_event(
            &mut transaction,
            &self.workspace_id,
            &self.run_id,
            run.next_event_sequence,
            NewEvent::MiddlewareEnded(id, status),
            now,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn current(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
    ) -> Result<(crate::lifecycle::RunRecord, i64), Error> {
        let run = run_repository::hold_run(transaction, &self.workspace_id, &self.run_id)
            .await?
            .ok_or_else(|| Error::RunEventPersistence("Program Run no longer exists".into()))?;
        let now = self.clock.now_unix_nanos().map_err(Error::Clock)?;
        if !run.program_work_is_current(&self.claim_id, now) {
            return Err(Error::RunEventPersistence(
                "Program Run no longer accepts Middleware evidence".into(),
            ));
        }
        Ok((run.clone(), now.max(run.accepted_at_unix_nanos)))
    }
}
