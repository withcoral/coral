use std::fmt;

use coral_workspace::{Principal, WorkspaceId};
use uuid::Uuid;

use crate::Error;

/// Maximum number of Unicode scalar values in one Task intent.
pub const TASK_INTENT_MAX_CHARS: usize = 80;

/// Maximum number of Unicode scalar values in optional Task completion feedback.
pub const TASK_FEEDBACK_MAX_CHARS: usize = 4096;

pub(crate) fn validate_feedback(value: &str) -> Result<(), Error> {
    if value.trim().is_empty()
        || value.contains('\0')
        || value.chars().count() > TASK_FEEDBACK_MAX_CHARS
    {
        return Err(Error::InvalidInput(format!(
            "task feedback must be non-blank, contain no null character, and be at most {TASK_FEEDBACK_MAX_CHARS} characters"
        )));
    }
    Ok(())
}

/// Stable Coral-generated Task identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TaskId(Uuid);

impl TaskId {
    pub(crate) fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    /// Parses a Task identity at an Adapter boundary.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] when `value` is not a canonical UUID.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let id = Uuid::parse_str(value)
            .map_err(|_parse_error| Error::InvalidInput("task id must be a UUID".to_string()))?;
        if id.hyphenated().to_string() != value {
            return Err(Error::InvalidInput(
                "task id must use canonical lowercase UUID form".to_string(),
            ));
        }
        Ok(Self(id))
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0.hyphenated())
    }
}

/// Bounded declared intent of one Task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskIntent(String);

impl TaskIntent {
    /// Validates a Task intent.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] when the intent is blank, contains a
    /// null character, or exceeds [`TASK_INTENT_MAX_CHARS`].
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.trim().is_empty()
            || value.contains('\0')
            || value.chars().count() > TASK_INTENT_MAX_CHARS
        {
            return Err(Error::InvalidInput(format!(
                "task intent must be non-blank, contain no null character, and be at most {TASK_INTENT_MAX_CHARS} characters"
            )));
        }
        Ok(Self(value.to_string()))
    }

    /// Returns the declared intent exactly as accepted.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Terminal Task outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOutcome {
    /// The declared unit of work succeeded.
    Success,
    /// The declared unit of work failed.
    Failure,
}

impl TaskOutcome {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "success" => Ok(Self::Success),
            "failure" => Ok(Self::Failure),
            _ => Err(Error::CorruptData(format!(
                "Task has unrecognized outcome '{value}'"
            ))),
        }
    }
}

/// A Task's summed context impact in one activity snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContextImpact {
    pub bytes: i64,
    /// A child Run is still running, so `bytes` covers only finished children.
    pub partial: bool,
}

/// Durable Task lifecycle evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    pub(crate) context_impact: Option<ContextImpact>,
    id: TaskId,
    workspace_id: WorkspaceId,
    principal: Principal,
    intent: TaskIntent,
    outcome: Option<TaskOutcome>,
    feedback: Option<String>,
    started_at_unix_nanos: i64,
    ended_at_unix_nanos: Option<i64>,
}

impl Task {
    #[expect(
        clippy::too_many_arguments,
        reason = "constructs one complete durable Task snapshot"
    )]
    pub(crate) const fn new(
        id: TaskId,
        workspace_id: WorkspaceId,
        principal: Principal,
        intent: TaskIntent,
        outcome: Option<TaskOutcome>,
        feedback: Option<String>,
        started_at_unix_nanos: i64,
        ended_at_unix_nanos: Option<i64>,
    ) -> Self {
        Self {
            context_impact: None,
            id,
            workspace_id,
            principal,
            intent,
            outcome,
            feedback,
            started_at_unix_nanos,
            ended_at_unix_nanos,
        }
    }

    /// Net retained discovery and execution payload impact in an activity snapshot.
    /// Authoring does not participate. While a child Run is running, this sums the
    /// finished children. A child that cannot be measured adds nothing. Absent if
    /// this is not an activity read.
    #[must_use]
    pub fn context_impact_bytes(&self) -> Option<i64> {
        self.context_impact.map(|impact| impact.bytes)
    }

    /// Whether a child Run is still running, so the impact covers only finished
    /// children and will change. False without an impact.
    #[must_use]
    pub fn context_impact_partial(&self) -> bool {
        self.context_impact.is_some_and(|impact| impact.partial)
    }

    /// Returns the Coral-generated stable identity.
    #[must_use]
    pub const fn id(&self) -> &TaskId {
        &self.id
    }

    /// Returns the Workspace confidentiality boundary.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    /// Returns the unchanged accountable Principal attribution.
    #[must_use]
    pub const fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Returns the declared intent.
    #[must_use]
    pub const fn intent(&self) -> &TaskIntent {
        &self.intent
    }

    /// Returns the terminal outcome, or `None` while the Task is active.
    #[must_use]
    pub const fn outcome(&self) -> Option<TaskOutcome> {
        self.outcome
    }

    /// Returns optional completion feedback, private with this Task.
    #[must_use]
    pub fn feedback(&self) -> Option<&str> {
        self.feedback.as_deref()
    }

    /// Returns when Coral durably started the Task.
    #[must_use]
    pub const fn started_at_unix_nanos(&self) -> i64 {
        self.started_at_unix_nanos
    }

    /// Returns when the terminal outcome committed.
    #[must_use]
    pub const fn ended_at_unix_nanos(&self) -> Option<i64> {
        self.ended_at_unix_nanos
    }
}
