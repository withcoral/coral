use std::fmt;

use coral_workspace::{Principal, WorkspaceId};

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

/// Stable Coral-generated Task identity: four words from Coral's word list,
/// such as `random-big-business-store`, or the UUID of a Task started before
/// Coral issued words. The text is the stored primary key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TaskId(String);

impl TaskId {
    /// Draws a fresh identifier. Only a successful insert proves it unused.
    pub(crate) fn generate() -> Result<Self, Error> {
        coral_identifier::generate()
            .map(Self)
            .map_err(|error| Error::IdentifierIssuance(error.to_string()))
    }

    /// Parses a Task identity at an Adapter boundary.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] unless `value` is four or five lowercase
    /// words joined by hyphens, or a canonical lowercase UUID.
    pub fn parse(value: &str) -> Result<Self, Error> {
        coral_identifier::validate(value)
            .map_err(|error| Error::InvalidInput(format!("task id is invalid: {error}")))?;
        Ok(Self(value.to_string()))
    }

    /// The identifier text, as stored and shown.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
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

/// Fewest characters an Activity intent search accepts.
pub const INTENT_QUERY_MIN_CHARS: usize = 3;
/// Most characters an Activity intent search accepts. A query longer than
/// [`TASK_INTENT_MAX_CHARS`] matches nothing; this only bounds the input.
pub const INTENT_QUERY_MAX_CHARS: usize = 1024;

/// A literal Activity search over Task intent and Run submission intent.
///
/// Matching is substring containment that ignores ASCII case, the same on
/// SQLite and Postgres. Other characters match exactly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntentQuery(String);

impl IntentQuery {
    /// Trims surrounding whitespace and validates the query.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] when the trimmed query has fewer than
    /// [`INTENT_QUERY_MIN_CHARS`] or more than [`INTENT_QUERY_MAX_CHARS`]
    /// characters, or contains a null character.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let value = value.trim();
        let length = value.chars().count();
        if !(INTENT_QUERY_MIN_CHARS..=INTENT_QUERY_MAX_CHARS).contains(&length)
            || value.contains('\0')
        {
            return Err(Error::InvalidInput(format!(
                "intent query must be {INTENT_QUERY_MIN_CHARS} to {INTENT_QUERY_MAX_CHARS} characters with no null character"
            )));
        }
        Ok(Self(value.to_ascii_lowercase()))
    }

    /// Whether `intent` contains the query.
    #[must_use]
    pub fn matches(&self, intent: &TaskIntent) -> bool {
        intent.as_str().to_ascii_lowercase().contains(&self.0)
    }

    /// The query with ASCII letters in lowercase.
    pub(crate) fn folded(&self) -> &str {
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
    pub(crate) intent_match: Option<bool>,
    id: TaskId,
    workspace_id: WorkspaceId,
    principal: Principal,
    intent: TaskIntent,
    outcome: Option<TaskOutcome>,
    feedback: Option<String>,
    started_at_unix_nanos: i64,
    ended_at_unix_nanos: Option<i64>,
}

/// The ID of a Task Coral found, as a span field.
#[derive(Debug, Clone, Copy)]
pub struct FoundTaskId<'a>(&'a TaskId);

impl fmt::Display for FoundTaskId<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Only a Task Coral read makes one. A [`TaskId`] a request names is
/// caller text until Coral finds its Task: its words need not be Coral's.
impl coral_telemetry::safe::Identifier for FoundTaskId<'_> {}

impl Task {
    /// The Task's ID as a span field.
    #[must_use]
    pub const fn telemetry_id(&self) -> FoundTaskId<'_> {
        FoundTaskId(&self.id)
    }

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
            intent_match: None,
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

    /// Whether the Task's own intent contains an activity read's intent query.
    /// False when the read keeps the Task only for its matching Runs. Absent
    /// without a query.
    #[must_use]
    pub const fn intent_matches(&self) -> Option<bool> {
        self.intent_match
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
