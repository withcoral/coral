use std::fmt;
use std::time::Duration;

use base64::Engine as _;
use coral_task::TaskId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::Error;

/// The most characters one caller message may hold.
pub const AGENT_MESSAGE_MAX_CHARS: usize = 32_768;
/// The most Events one observation returns.
pub const TURN_EVENT_PAGE_MAX: u16 = 100;
/// The longest one observation may wait.
pub const TURN_WAIT_MAX: Duration = Duration::from_secs(30);

/// Stable Coral-generated identity of one Agent Turn.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TurnId(Uuid);

impl TurnId {
    pub(crate) fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    /// Parses a canonical identity at an Adapter boundary.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] unless `value` is a canonical lowercase UUID.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let id = Uuid::parse_str(value)
            .map_err(|_error| Error::InvalidInput("turn id must be a UUID".into()))?;
        if id.hyphenated().to_string() != value {
            return Err(Error::InvalidInput(
                "turn id must use canonical lowercase UUID form".into(),
            ));
        }
        Ok(Self(id))
    }
}

impl fmt::Display for TurnId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0.hyphenated())
    }
}

/// One caller message, bounded and nonblank.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMessage(String);

impl AgentMessage {
    /// Validates one caller message.
    ///
    /// # Errors
    ///
    /// Rejects blank messages, null characters, and messages over
    /// [`AGENT_MESSAGE_MAX_CHARS`].
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.trim().is_empty()
            || value.contains('\0')
            || value.chars().count() > AGENT_MESSAGE_MAX_CHARS
        {
            return Err(Error::InvalidInput(format!(
                "message must be nonblank, contain no null character, and be at most {AGENT_MESSAGE_MAX_CHARS} characters"
            )));
        }
        Ok(Self(value.to_owned()))
    }

    /// The message text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Lifecycle of one Agent Turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnState {
    /// The agent is working.
    Running,
    /// A person must act on a Run before the agent can go on.
    NeedsAction,
    /// The agent ended the turn with a final message.
    Completed,
    /// The turn could not finish.
    Failed,
    /// The caller canceled the turn.
    Canceled,
    /// The host stopped while the turn ran.
    Interrupted,
}

impl TurnState {
    /// Whether the turn has ended.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Running | Self::NeedsAction)
    }

    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::NeedsAction => "needs_action",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::Interrupted => "interrupted",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        Ok(match value {
            "running" => Self::Running,
            "needs_action" => Self::NeedsAction,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "canceled" => Self::Canceled,
            "interrupted" => Self::Interrupted,
            _ => return Err(Error::CorruptData("Agent Turn has unknown state".into())),
        })
    }
}

/// The Run behind a completed answer, so the caller can check and save it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Evidence {
    /// The answering Program Run.
    pub run_id: String,
    /// The Program source that Run executed.
    pub program: String,
    /// The Run's Final Value, so the caller can check the answer against it.
    /// One whose JSON passes [`EVIDENCE_VALUE_MAX_BYTES`] is kept as the start
    /// of that JSON, a string, with `value_truncated` set.
    #[serde(default)]
    pub value: Option<Value>,
    /// Whether `value` holds only the start of the Final Value's JSON.
    #[serde(default)]
    pub value_truncated: bool,
}

/// The most Final Value JSON that evidence keeps.
pub const EVIDENCE_VALUE_MAX_BYTES: usize = 16 * 1024;

impl Evidence {
    /// Evidence for a succeeded Run, keeping as much of its Final Value as fits.
    #[must_use]
    pub fn new(run_id: String, program: String, final_value: Option<&Value>) -> Self {
        let (value, value_truncated) = match final_value {
            None => (None, false),
            Some(value) => {
                let json = value.to_string();
                if json.len() <= EVIDENCE_VALUE_MAX_BYTES {
                    (Some(value.clone()), false)
                } else {
                    let end = json.floor_char_boundary(EVIDENCE_VALUE_MAX_BYTES);
                    let start = json.get(..end).unwrap_or_default();
                    (Some(Value::String(start.to_owned())), true)
                }
            }
        };
        Self {
            run_id,
            program,
            value,
            value_truncated,
        }
    }
}

/// What one turn Event records.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TurnEventKind {
    /// The agent called one tool, for the reason it gave.
    Step {
        /// The tool name.
        tool: String,
        /// The reason the agent gave, when the tool takes one.
        intent: Option<String>,
    },
    /// A Run the agent started is waiting for a person.
    NeedsAction(RequiredAction),
}

impl TurnEventKind {
    pub(crate) const fn as_storage_str(&self) -> &'static str {
        match self {
            Self::Step { .. } => "step",
            Self::NeedsAction { .. } => "needs_action",
        }
    }
}

/// One retained turn Event.
#[derive(Debug, Clone, PartialEq)]
pub struct TurnEvent {
    pub(crate) sequence: u64,
    pub(crate) occurred_at_unix_nanos: i64,
    pub(crate) kind: TurnEventKind,
}

impl TurnEvent {
    /// Position in the turn, from 1.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// When it happened.
    #[must_use]
    pub const fn occurred_at_unix_nanos(&self) -> i64 {
        self.occurred_at_unix_nanos
    }

    /// What happened.
    #[must_use]
    pub const fn kind(&self) -> &TurnEventKind {
        &self.kind
    }
}

const TURN_CURSOR_FORMAT: u8 = 0xa1;
const TURN_CURSOR_PAYLOAD_BYTES: usize = 1 + 16 + 8;

/// Opaque, non-authorizing observation position in one turn's Events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnCursor {
    turn_id: TurnId,
    event_sequence: u64,
}

impl TurnCursor {
    pub(crate) const fn new(turn_id: TurnId, event_sequence: u64) -> Self {
        Self {
            turn_id,
            event_sequence,
        }
    }

    /// Parses the opaque value returned by [`fmt::Display`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for malformed or noncanonical input.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let invalid = || Error::InvalidInput("turn cursor is invalid".into());
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_error| invalid())?;
        if bytes.len() != TURN_CURSOR_PAYLOAD_BYTES
            || bytes.first() != Some(&TURN_CURSOR_FORMAT)
            || base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&bytes) != value
        {
            return Err(invalid());
        }
        let id = bytes.get(1..17).ok_or_else(invalid)?;
        let sequence = bytes.get(17..25).ok_or_else(invalid)?;
        let turn_id = TurnId(Uuid::from_slice(id).map_err(|_error| invalid())?);
        let event_sequence = u64::from_be_bytes(sequence.try_into().map_err(|_error| invalid())?);
        Ok(Self::new(turn_id, event_sequence))
    }

    pub(crate) const fn turn_id(&self) -> &TurnId {
        &self.turn_id
    }

    pub(crate) const fn event_sequence(&self) -> u64 {
        self.event_sequence
    }
}

impl fmt::Display for TurnCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut bytes = Vec::with_capacity(TURN_CURSOR_PAYLOAD_BYTES);
        bytes.push(TURN_CURSOR_FORMAT);
        bytes.extend_from_slice(self.turn_id.0.as_bytes());
        bytes.extend_from_slice(&self.event_sequence.to_be_bytes());
        formatter.write_str(&base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
    }
}

/// One bounded observation of a turn's Events and state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnObservation {
    cursor: Option<TurnCursor>,
    event_limit: u16,
    wait: Duration,
    batch_events: bool,
}

impl TurnObservation {
    /// Observes at once, after an optional cursor.
    #[must_use]
    pub const fn immediate(cursor: Option<TurnCursor>) -> Self {
        Self {
            cursor,
            event_limit: TURN_EVENT_PAGE_MAX,
            wait: Duration::ZERO,
            batch_events: false,
        }
    }

    /// Observes a turn that just started. Progress Events wait for the end of
    /// the turn, a required action, or the wait, so the first page is not one step.
    ///
    /// # Errors
    ///
    /// Rejects waits longer than [`TURN_WAIT_MAX`].
    pub fn initial(wait: Duration) -> Result<Self, Error> {
        let mut observation = Self::immediate(None).with_wait(wait)?;
        observation.batch_events = true;
        Ok(observation)
    }

    /// Waits until new Events, a required action, or the end of the turn.
    ///
    /// # Errors
    ///
    /// Rejects waits longer than [`TURN_WAIT_MAX`].
    pub fn with_wait(mut self, wait: Duration) -> Result<Self, Error> {
        if wait > TURN_WAIT_MAX {
            return Err(Error::InvalidInput(
                "turn observation wait must not exceed 30 seconds".into(),
            ));
        }
        self.wait = wait;
        Ok(self)
    }

    pub(crate) const fn wait(&self) -> Duration {
        self.wait
    }

    pub(crate) const fn cursor(&self) -> Option<&TurnCursor> {
        self.cursor.as_ref()
    }

    pub(crate) const fn event_limit(&self) -> u16 {
        self.event_limit
    }

    /// Ready at the end of the turn or on new Events. A batching observation
    /// waits past progress Events and stops only for a required action.
    pub(crate) fn is_ready(&self, update: &TurnUpdate) -> bool {
        let after = self.cursor().map_or(0, TurnCursor::event_sequence);
        update.state.is_terminal()
            || update
                .events
                .iter()
                .filter(|event| event.sequence > after)
                .any(|event| {
                    !self.batch_events || matches!(event.kind, TurnEventKind::NeedsAction(_))
                })
    }
}

/// Current state, a bounded page of Events, and the result once the turn ends.
#[derive(Debug, Clone, PartialEq)]
pub struct TurnUpdate {
    pub(crate) turn_id: TurnId,
    pub(crate) task_id: TaskId,
    pub(crate) state: TurnState,
    pub(crate) events: Vec<TurnEvent>,
    pub(crate) next_cursor: TurnCursor,
    pub(crate) has_more: bool,
    pub(crate) final_message: Option<String>,
    pub(crate) evidence: Option<Evidence>,
    pub(crate) failure: Option<String>,
    pub(crate) required_action: Option<RequiredAction>,
}

/// A Run the agent started that waits for a person.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequiredAction {
    /// The waiting Run.
    pub run_id: String,
    /// The Run's required actions, as its own page shows them.
    pub required_actions: Value,
}

impl TurnUpdate {
    /// The observed turn.
    #[must_use]
    pub const fn turn_id(&self) -> &TurnId {
        &self.turn_id
    }

    /// The Task whose thread holds the turn.
    #[must_use]
    pub const fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// Current state.
    #[must_use]
    pub const fn state(&self) -> TurnState {
        self.state
    }

    /// Events after the cursor, oldest first.
    #[must_use]
    pub fn events(&self) -> &[TurnEvent] {
        &self.events
    }

    /// Where the next observation continues.
    #[must_use]
    pub const fn next_cursor(&self) -> &TurnCursor {
        &self.next_cursor
    }

    /// Whether more retained Events follow this page.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// The agent's final message, on the last page of a completed turn.
    #[must_use]
    pub fn final_message(&self) -> Option<&str> {
        self.final_message.as_deref()
    }

    /// The answering Run, when the completed turn had one.
    #[must_use]
    pub const fn evidence(&self) -> Option<&Evidence> {
        self.evidence.as_ref()
    }

    /// Why the turn failed or was interrupted, on its last page.
    #[must_use]
    pub fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }

    /// The Run and actions a person must act on, while the turn needs action.
    #[must_use]
    pub const fn required_action(&self) -> Option<&RequiredAction> {
        self.required_action.as_ref()
    }
}
