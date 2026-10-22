use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use base64::Engine as _;
use coral_workspace::Principal;
use coral_workspace::WorkspaceId;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::Error;

/// Maximum UTF-8 byte length of one submitted Program body.
pub const PROGRAM_SOURCE_MAX_BYTES: usize = 64 * 1024;
/// Maximum number of distinct Saved Functions in one linked source closure.
pub const LINKED_SAVED_FUNCTION_MAX_COUNT: usize = 64;
/// Maximum combined UTF-8 bytes in submitted and linked TypeScript source.
pub const COMBINED_TYPESCRIPT_MAX_BYTES: usize = 512 * 1024;
/// Maximum UTF-8 byte length of emitted JavaScript in one accepted package.
pub const EMITTED_JAVASCRIPT_MAX_BYTES: usize = 1024 * 1024;
/// Maximum UTF-8 byte length of one accepted source map.
pub const SOURCE_MAP_MAX_BYTES: usize = 2 * 1024 * 1024;
/// Maximum wall-clock duration of one Program compilation.
pub const COMPILATION_TIMEOUT: Duration = coral_v8::COMPILATION_TIMEOUT;
/// Fixed v1 maximum Run timeout, bounding how long one Program can occupy a worker.
pub const RUN_TIMEOUT_MAX_MS: u64 = 900_000;
/// Maximum number of authored Emissions/Logs returned by one observation.
pub const RUN_EVENT_PAGE_MAX: u16 = 100;
/// Maximum UTF-8 byte length of one Program Log message.
pub const PROGRAM_LOG_MESSAGE_MAX_BYTES: usize = 64 * 1024;
/// Events after which the Run Owner closes a Run Ledger chunk.
pub const CHUNK_EVENTS_CAP: usize = 1000;
/// Output bytes after which the Run Owner closes a Run Ledger chunk; one larger
/// Event commits alone.
pub const CHUNK_BYTES_CAP: usize = 1024 * 1024;

const RUN_CURSOR_FORMAT: u8 = 4;
const RUN_CURSOR_PAYLOAD_BYTES: usize = 1 + RUN_DIGEST_BYTES + 8 + 32;
const RUN_DIGEST_BYTES: usize = 16;
const RUN_CURSOR_TOKEN_BYTES: usize = 76;

/// Stable Coral-generated Program Run identity: four words from Coral's word
/// list, such as `height-word-database-library`, or the UUID of a Run accepted
/// before Coral issued words. The text is the stored primary key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RunId(String);

impl RunId {
    /// Draws a fresh identifier. Only a successful insert proves it unused.
    pub(crate) fn generate() -> Result<Self, Error> {
        coral_identifier::generate()
            .map(Self)
            .map_err(|error| Error::IdentifierIssuance(error.to_string()))
    }

    /// Parses a Run identity at an Adapter boundary.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] unless `value` is four or five lowercase
    /// words joined by hyphens, or a canonical lowercase UUID.
    pub fn parse(value: &str) -> Result<Self, Error> {
        coral_identifier::validate(value)
            .map_err(|error| Error::InvalidInput(format!("run id is invalid: {error}")))?;
        Ok(Self(value.to_string()))
    }

    /// The identifier text, as stored and shown.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Stable Coral-generated Operation Invocation identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InvocationId(Uuid);

impl InvocationId {
    pub(crate) fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    /// Parses an Invocation identity at an Adapter boundary.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] when `value` is not a canonical UUID.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let id = Uuid::parse_str(value)
            .map_err(|_error| Error::InvalidInput("invocation id must be a UUID".to_string()))?;
        if id.hyphenated().to_string() != value {
            return Err(Error::InvalidInput(
                "invocation id must use canonical lowercase UUID form".to_string(),
            ));
        }
        Ok(Self(id))
    }
}

impl fmt::Display for InvocationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0.hyphenated())
    }
}

/// Exact submitted TypeScript async-function body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgramSource(String);

impl ProgramSource {
    /// Applies the fixed submitted-source admission limit.
    ///
    /// Empty input is valid because an empty async-function body is a valid
    /// Program with no Final Value.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] when the source contains a null byte or
    /// exceeds [`PROGRAM_SOURCE_MAX_BYTES`].
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.contains('\0') {
            return Err(Error::InvalidInput(
                "Program source must contain no null byte".to_string(),
            ));
        }
        if value.len() > PROGRAM_SOURCE_MAX_BYTES {
            return Err(Error::InvalidInput(format!(
                "Program source must be at most {PROGRAM_SOURCE_MAX_BYTES} UTF-8 bytes"
            )));
        }
        Ok(Self(value.to_string()))
    }

    /// Returns the exact accepted body.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Positive effective timeout for one accepted Program Run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunTimeout {
    nanoseconds: i64,
}

impl RunTimeout {
    /// Builds a positive timeout from milliseconds.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for zero or a value above [`RUN_TIMEOUT_MAX_MS`].
    pub fn from_millis(milliseconds: u64) -> Result<Self, Error> {
        if milliseconds > RUN_TIMEOUT_MAX_MS {
            return Err(Error::InvalidInput(format!(
                "Run timeout must be at most {RUN_TIMEOUT_MAX_MS} milliseconds"
            )));
        }
        let nanoseconds = milliseconds
            .checked_mul(1_000_000)
            .and_then(|value| i64::try_from(value).ok())
            .filter(|value| *value > 0)
            .ok_or_else(|| {
                Error::InvalidInput(
                    "Run timeout must be positive and fit durable nanoseconds".to_string(),
                )
            })?;
        Ok(Self { nanoseconds })
    }

    pub(crate) const fn nanoseconds(self) -> i64 {
        self.nanoseconds
    }
}

/// Complete caller submission needed for core Program admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSubmission {
    source: ProgramSource,
    timeout: RunTimeout,
}

impl RunSubmission {
    /// Builds one Program submission.
    #[must_use]
    pub const fn new(source: ProgramSource, timeout: RunTimeout) -> Self {
        Self { source, timeout }
    }

    /// Returns the exact submitted Program source.
    #[must_use]
    pub const fn source(&self) -> &ProgramSource {
        &self.source
    }

    /// Returns the effective Run timeout.
    #[must_use]
    pub const fn timeout(&self) -> RunTimeout {
        self.timeout
    }
}

/// Public lifecycle state of one accepted Program Run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    /// Accepted work has not reached a terminal result.
    Running,
    /// Program execution fulfilled and all core output is durable.
    Succeeded,
    /// Program execution failed.
    Failed,
    /// Durable cancellation won the terminal race.
    Canceled,
    /// The accepted deadline won the terminal race.
    TimedOut,
    /// Started execution was lost and will not be replayed.
    Interrupted,
}

impl RunState {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::TimedOut => "timed_out",
            Self::Interrupted => "interrupted",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "canceled" => Ok(Self::Canceled),
            "timed_out" => Ok(Self::TimedOut),
            "interrupted" => Ok(Self::Interrupted),
            _ => Err(Error::CorruptData(format!(
                "Run has unrecognized state '{value}'"
            ))),
        }
    }

    /// Returns whether the state is immutable and terminal.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Running)
    }
}

/// Bounded native JSON projection returned by a successful Program.
#[derive(Debug, Clone, PartialEq)]
pub struct ProgramValue {
    json: Value,
    encoded_json: String,
}

impl ProgramValue {
    #[cfg(test)]
    pub(crate) fn from_encoded_json(encoded_json: String) -> Result<Self, Error> {
        Self::with_limits(encoded_json, coral_value::ValueLimits::default())
    }

    pub(crate) fn with_limits(
        encoded_json: String,
        limits: coral_value::ValueLimits,
    ) -> Result<Self, Error> {
        let json = limits
            .validate_json(&encoded_json)
            .map_err(|error| match error {
                coral_value::ValueError::Invalid => Error::InvalidProgramValue,
                coral_value::ValueError::TooLarge => Error::ProgramValueTooLarge,
            })?;
        Ok(Self { json, encoded_json })
    }

    pub(crate) fn from_persisted_json(
        encoded_json: String,
        limits: coral_value::ValueLimits,
    ) -> Result<Self, Error> {
        Self::with_limits(encoded_json, limits)
            .map_err(|error| Error::CorruptData(format!("Run has invalid Program Value: {error}")))
    }

    #[cfg(test)]
    pub(crate) fn from_json(json: &Value) -> Result<Self, Error> {
        let encoded_json = serde_json::to_string(json).map_err(Error::ProgramValueJson)?;
        Self::from_encoded_json(encoded_json)
    }

    /// Returns the parsed JSON domain value.
    #[must_use]
    pub const fn as_json(&self) -> &Value {
        &self.json
    }

    pub(crate) fn encoded_json(&self) -> &str {
        &self.encoded_json
    }

    /// The exact encoded JSON, as the row stores it.
    pub(crate) fn into_encoded_json(self) -> String {
        self.encoded_json
    }
}

/// Successful Program result, preserving absent versus explicit null.
#[derive(Debug, Clone, PartialEq)]
pub enum FinalValue {
    /// The fulfilled Program returned JavaScript `undefined`.
    Absent,
    /// The Program returned a value with a present native JSON projection.
    Present(ProgramValue),
}

/// Stable safe category for a failed Program Run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunFailureCode {
    /// The Program entry Promise rejected.
    ProgramRejected,
    /// The Program returned a value outside the core Program Value contract.
    InvalidProgramValue,
    /// A value or cumulative output limit was exceeded.
    ResourceLimitExceeded,
    /// The Program exceeded its execution isolate's heap budget.
    MemoryLimitExceeded,
    /// The isolated runtime could not complete execution safely.
    ProgramExecutionFailed,
    /// Current authority disappeared before Program execution started.
    RunAuthorityLost,
}

/// Safe terminal Program failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunFailure {
    code: RunFailureCode,
    message: String,
    error: Option<coral_v8::ProgramError>,
}

impl RunFailure {
    pub(crate) const fn new(code: RunFailureCode, message: String) -> Self {
        Self {
            code,
            message,
            error: None,
        }
    }

    pub(crate) fn with_error(mut self, error: coral_v8::ProgramError) -> Self {
        self.error = Some(error);
        self
    }

    /// Returns the existing isolate-visible error, when execution produced one.
    #[must_use]
    pub const fn error(&self) -> Option<&coral_v8::ProgramError> {
        self.error.as_ref()
    }

    /// Returns the stable failure category.
    #[must_use]
    pub const fn code(&self) -> RunFailureCode {
        self.code
    }

    /// Returns the safe diagnostic message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Immutable terminal result of one Program Run.
#[derive(Debug, Clone, PartialEq)]
pub enum RunTerminalOutcome {
    /// Program execution fulfilled with an optional Final Value.
    Succeeded {
        /// Distinguishes JavaScript `undefined` from a present Program Value.
        final_value: FinalValue,
    },
    /// Program execution failed with a safe diagnostic.
    Failed(RunFailure),
    /// Durable cancellation won.
    Canceled,
    /// The durable Run deadline won.
    TimedOut,
    /// Started execution was lost without replay.
    Interrupted,
}

impl RunTerminalOutcome {
    /// Returns the matching public terminal state.
    #[must_use]
    pub const fn state(&self) -> RunState {
        match self {
            Self::Succeeded { .. } => RunState::Succeeded,
            Self::Failed(_) => RunState::Failed,
            Self::Canceled => RunState::Canceled,
            Self::TimedOut => RunState::TimedOut,
            Self::Interrupted => RunState::Interrupted,
        }
    }
}

/// Durable terminal category of one Operation Invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvocationResultKind {
    /// One complete upstream response was persisted.
    UpstreamOutcome,
    /// One known safe Coral Failure was persisted.
    CoralFailure,
    /// Run termination won before dispatch.
    RunTerminated,
    /// Dispatch started but no complete local result won durably.
    Indeterminate,
}

impl InvocationResultKind {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::UpstreamOutcome => "upstream_outcome",
            Self::CoralFailure => "coral_failure",
            Self::RunTerminated => "run_terminated",
            Self::Indeterminate => "indeterminate",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "upstream_outcome" => Ok(Self::UpstreamOutcome),
            "coral_failure" => Ok(Self::CoralFailure),
            "run_terminated" => Ok(Self::RunTerminated),
            "indeterminate" => Ok(Self::Indeterminate),
            _ => Err(Error::CorruptData(format!(
                "Invocation Event has unrecognized result kind '{value}'"
            ))),
        }
    }
}

/// The accepted source of one Run that holds a [`SourceLocation`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceDocument {
    /// The Run's exact submitted Program source.
    Program,
    /// The Run's retained snapshot of one linked Saved Function.
    SavedFunction {
        /// Saved Function identity of the snapshot, unique within the Run.
        saved_function_id: String,
    },
}

/// Where the Run's accepted source made one Operation call, Emission, or Log.
///
/// Coral records the innermost authored frame when the call happens. Line and
/// column start at 1; the column counts UTF-16 code units, as V8 and
/// TypeScript do. Records made without one, including every record written
/// before Coral captured locations, have no location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceLocation {
    /// Accepted source that holds the call.
    pub document: SourceDocument,
    /// One-based line.
    pub line: u32,
    /// One-based column in UTF-16 code units.
    pub column: u32,
}

/// One immutable Program Run Event.
#[derive(Debug, Clone, PartialEq)]
pub struct RunEvent {
    sequence: u64,
    kind: RunEventKind,
    occurred_at_unix_nanos: i64,
    source_location: Option<SourceLocation>,
}

impl RunEvent {
    pub(crate) const fn new(
        sequence: u64,
        kind: RunEventKind,
        occurred_at_unix_nanos: i64,
    ) -> Self {
        Self {
            sequence,
            kind,
            occurred_at_unix_nanos,
            source_location: None,
        }
    }

    /// Returns the one-based sequence within the Run.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the durable Event kind and safe payload.
    #[must_use]
    pub const fn kind(&self) -> &RunEventKind {
        &self.kind
    }

    /// Returns when the Event committed.
    #[must_use]
    pub const fn occurred_at_unix_nanos(&self) -> i64 {
        self.occurred_at_unix_nanos
    }

    pub(crate) fn with_source_location(mut self, location: Option<SourceLocation>) -> Self {
        self.source_location = location;
        self
    }

    /// Returns where authored source made an Emission or Log, when recorded.
    /// Always `None` for other Event kinds.
    #[must_use]
    pub const fn source_location(&self) -> Option<&SourceLocation> {
        self.source_location.as_ref()
    }
}

/// Durable v1 Program Run Event kinds.
///
/// Adapters must preserve a fallback when projecting this non-exhaustive set so
/// a later Event type does not prevent clients from reading the rest of a page.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum RunEventKind {
    /// The complete package and Run were durably accepted.
    Started,
    /// One resolved Operation call became a durable Invocation.
    InvocationStarted {
        /// Stable child Invocation identity.
        invocation_id: InvocationId,
    },
    /// One Invocation result became durable before its Program Promise settled.
    InvocationEnded {
        /// Stable child Invocation identity.
        invocation_id: InvocationId,
        /// Durable terminal Invocation category.
        result_kind: InvocationResultKind,
    },
    /// One Program Value became durable caller-facing output.
    ProgramEmitted {
        /// Exact bounded Program Value published by the Program.
        value: ProgramValue,
    },
    /// One diagnostic Program Log became durable separately from output.
    ProgramLogged {
        /// Stable severity selected through the bounded console surface.
        level: ProgramLogLevel,
        /// Bounded diagnostic message.
        message: String,
    },
    /// One authorized Principal requested cancellation.
    CancellationRequested {
        /// Accountable Principal that requested cancellation.
        principal: Principal,
    },
    /// The first terminal result committed.
    Ended {
        /// Immutable terminal Run state.
        state: RunState,
    },
}

/// Severity of one durable Program Log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgramLogLevel {
    /// Diagnostic detail requested through `console.debug`.
    Debug,
    /// Informational output requested through `console.info` or `console.log`.
    Info,
    /// Warning output requested through `console.warn`.
    Warn,
    /// Error-level diagnostics requested through `console.error`.
    Error,
}

impl ProgramLogLevel {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            _ => Err(Error::CorruptData(format!(
                "Program Log has unrecognized level '{value}'"
            ))),
        }
    }
}

/// Opaque, non-authorizing authored-output observation position.
/// The retained Event sequence can skip internal records; human history uses
/// its own numeric Event positions, never these tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunCursor {
    /// The first 16 bytes of the SHA-256 of the Run identifier text, so the
    /// token stays fixed-size whatever the identifier's length.
    run_digest: [u8; RUN_DIGEST_BYTES],
    event_sequence: u64,
    required_action_revision: [u8; 32],
}

impl RunCursor {
    pub(crate) fn new(
        run_id: &RunId,
        event_sequence: u64,
        required_action_revision: [u8; 32],
    ) -> Self {
        Self::from_parts(run_digest(run_id), event_sequence, required_action_revision)
    }

    const fn from_parts(
        run_digest: [u8; RUN_DIGEST_BYTES],
        event_sequence: u64,
        required_action_revision: [u8; 32],
    ) -> Self {
        Self {
            run_digest,
            event_sequence,
            required_action_revision,
        }
    }

    /// Whether this cursor was issued for `run_id`.
    pub(crate) fn belongs_to(&self, run_id: &RunId) -> bool {
        self.run_digest == run_digest(run_id)
    }

    /// Parses the opaque value returned by [`fmt::Display`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for malformed or noncanonical input.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.len() != RUN_CURSOR_TOKEN_BYTES {
            return Err(invalid_cursor());
        }
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_error| invalid_cursor())?;
        let Some((&format, payload)) = bytes.split_first() else {
            return Err(invalid_cursor());
        };
        if bytes.len() != RUN_CURSOR_PAYLOAD_BYTES
            || format != RUN_CURSOR_FORMAT
            || base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&bytes) != value
        {
            return Err(invalid_cursor());
        }
        let (run_digest, positions) = payload
            .split_at_checked(RUN_DIGEST_BYTES)
            .ok_or_else(invalid_cursor)?;
        let (event_sequence, required_action_revision) =
            positions.split_at_checked(8).ok_or_else(invalid_cursor)?;
        let run_digest = run_digest.try_into().map_err(|_error| invalid_cursor())?;
        let event_sequence = u64::from_be_bytes(
            event_sequence
                .try_into()
                .map_err(|_error| invalid_cursor())?,
        );
        let required_action_revision = required_action_revision
            .try_into()
            .map_err(|_error| invalid_cursor())?;
        Ok(Self::from_parts(
            run_digest,
            event_sequence,
            required_action_revision,
        ))
    }

    pub(crate) const fn event_sequence(&self) -> u64 {
        self.event_sequence
    }

    pub(crate) const fn required_action_revision(&self) -> [u8; 32] {
        self.required_action_revision
    }
}

impl fmt::Display for RunCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut bytes = Vec::with_capacity(RUN_CURSOR_PAYLOAD_BYTES);
        bytes.push(RUN_CURSOR_FORMAT);
        bytes.extend_from_slice(&self.run_digest);
        bytes.extend_from_slice(&self.event_sequence.to_be_bytes());
        bytes.extend_from_slice(&self.required_action_revision);
        formatter.write_str(&base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
    }
}

fn run_digest(run_id: &RunId) -> [u8; RUN_DIGEST_BYTES] {
    let digest = Sha256::digest(run_id.as_str().as_bytes());
    let mut prefix = [0_u8; RUN_DIGEST_BYTES];
    for (slot, byte) in prefix.iter_mut().zip(digest.iter()) {
        *slot = *byte;
    }
    prefix
}

fn invalid_cursor() -> Error {
    Error::InvalidInput("Run cursor is invalid".to_string())
}

/// One bounded observation of authored Emissions/Logs and current visible actions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunObservation {
    cursor: Option<RunCursor>,
    event_limit: u16,
    wait: Duration,
    batch_output: bool,
}

impl RunObservation {
    /// Builds one immediate observation after an optional cursor.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] unless `event_limit` is from one through
    /// [`RUN_EVENT_PAGE_MAX`].
    pub fn immediate(cursor: Option<RunCursor>, event_limit: u16) -> Result<Self, Error> {
        if event_limit == 0 || event_limit > RUN_EVENT_PAGE_MAX {
            return Err(Error::InvalidInput(format!(
                "Run Event limit must be from 1 through {RUN_EVENT_PAGE_MAX}"
            )));
        }
        Ok(Self {
            cursor,
            event_limit,
            wait: Duration::ZERO,
            batch_output: false,
        })
    }

    /// Observes a newly accepted Run, batching authored output until terminal state,
    /// a required human action, or the bounded initial wait expires.
    /// # Errors
    /// Rejects waits longer than 30 seconds.
    pub fn initial(wait: Duration) -> Result<Self, Error> {
        let mut observation = Self::immediate(None, RUN_EVENT_PAGE_MAX)?.with_wait(wait)?;
        observation.batch_output = true;
        Ok(observation)
    }

    /// The last Event a wait has seen before it starts: its cursor's, or without
    /// one, the last output its first read returned. Replayed output is not new.
    pub(crate) fn seen_sequence(&self, first: &RunUpdate) -> u64 {
        self.cursor().map_or_else(
            || first.events().last().map_or(0, RunEvent::sequence),
            RunCursor::event_sequence,
        )
    }

    pub(crate) fn is_ready(&self, update: &RunUpdate, seen_sequence: u64) -> bool {
        update.state().is_terminal()
            || (!self.batch_output
                && update
                    .events()
                    .last()
                    .is_some_and(|event| event.sequence() > seen_sequence))
            || update.next_cursor().required_action_revision()
                != self
                    .cursor()
                    .map_or([0; 32], RunCursor::required_action_revision)
    }

    /// Waits until new authored Emissions/Logs, changed actions, or terminal data are available.
    /// Without a cursor, output that exists when the wait begins is replayed, not new.
    /// A zero duration keeps the observation immediate.
    /// # Errors
    /// Rejects waits longer than 30 seconds.
    pub fn with_wait(mut self, wait: Duration) -> Result<Self, Error> {
        if wait > Duration::from_secs(30) {
            return Err(Error::InvalidInput(
                "Run observation wait must not exceed 30 seconds".into(),
            ));
        }
        self.wait = wait;
        Ok(self)
    }

    pub(crate) const fn wait(&self) -> Duration {
        self.wait
    }

    pub(crate) const fn cursor(&self) -> Option<&RunCursor> {
        self.cursor.as_ref()
    }

    pub(crate) const fn event_limit(&self) -> u16 {
        self.event_limit
    }
}

/// Current state, bounded replayable authored Emissions/Logs, actions, and terminal result.
#[derive(Debug, Clone, PartialEq)]
pub struct RunUpdate {
    run_id: RunId,
    value_limits: coral_value::ValueLimits,
    state: RunState,
    pub(crate) required_actions: Vec<crate::RequiredAction>,
    events: Vec<RunEvent>,
    next_cursor: RunCursor,
    has_more: bool,
    terminal_outcome: Option<RunTerminalOutcome>,
}

impl RunUpdate {
    pub(crate) fn new(
        run_id: RunId,
        state: RunState,
        events: Vec<RunEvent>,
        next_cursor: RunCursor,
        has_more: bool,
        terminal_outcome: Option<RunTerminalOutcome>,
        value_limits: coral_value::ValueLimits,
    ) -> Result<Self, Error> {
        if state.is_terminal()
            != terminal_outcome
                .as_ref()
                .is_some_and(|outcome| outcome.state() == state)
        {
            return Err(Error::CorruptData(
                "Run Update has inconsistent terminal evidence".to_string(),
            ));
        }
        Ok(Self {
            run_id,
            state,
            value_limits,
            required_actions: Vec::new(),
            events,
            next_cursor,
            has_more,
            terminal_outcome,
        })
    }

    /// Returns the effective limits retained at Run admission.
    #[must_use]
    pub const fn value_limits(&self) -> coral_value::ValueLimits {
        self.value_limits
    }

    /// Returns the stable accepted Run identity.
    #[must_use]
    pub const fn run_id(&self) -> &RunId {
        &self.run_id
    }

    /// Keeps a complete prefix without acknowledging omitted Emissions/Logs.
    /// Terminal data is deferred while authored records remain. Hidden records
    /// before the first omitted occurrence may be acknowledged.
    #[must_use]
    pub fn event_prefix(&self, count: usize) -> Self {
        let mut update = self.clone();
        if count < update.events.len() {
            if let Some(first_omitted) = update.events.get(count) {
                update.next_cursor.event_sequence = first_omitted.sequence() - 1;
            }
            update.events.truncate(count);
            update.has_more = true;
        }
        update
    }

    /// Defers terminal data to the observation after this nonempty authored page.
    /// The cursor advances over these records so the next observation can return
    /// the complete outcome on its own. Empty pages cannot defer terminal data.
    #[must_use]
    pub fn defer_terminal_outcome(mut self) -> Self {
        if !self.events.is_empty() && self.terminal_outcome.is_some() {
            self.has_more = true;
        }
        self
    }

    /// Returns the current public lifecycle state.
    #[must_use]
    pub const fn state(&self) -> RunState {
        self.state
    }

    /// Returns current actions visible to this Principal.
    #[must_use]
    pub fn required_actions(&self) -> &[crate::RequiredAction] {
        &self.required_actions
    }

    /// Returns the bounded authored Emission/Log page after the supplied cursor.
    /// Original Event sequences remain stable; gaps are hidden lifecycle records.
    #[must_use]
    pub fn events(&self) -> &[RunEvent] {
        &self.events
    }

    /// Returns the opaque position after this authored page and action snapshot.
    #[must_use]
    pub const fn next_cursor(&self) -> &RunCursor {
        &self.next_cursor
    }

    /// Returns whether authored Emissions/Logs or terminal data remain after this page.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the complete terminal data on the final page only.
    #[must_use]
    pub const fn terminal_outcome(&self) -> Option<&RunTerminalOutcome> {
        if self.has_more {
            None
        } else {
            self.terminal_outcome.as_ref()
        }
    }
}

/// Exact immutable attribution, deadline and claim retained by a durable Run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcceptedRun {
    pub(crate) id: RunId,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) principal: Principal,
    pub(crate) accepted_at_unix_nanos: i64,
    pub(crate) deadline_at_unix_nanos: i64,
    pub(crate) shutdown_grace_nanos: i64,
    pub(crate) value_limits: coral_value::ValueLimits,
    /// The Run Owner's claim, written as the worker claim by acceptance.
    pub(crate) claim_id: String,
    /// The first lease, which the owner renews with every chunk it commits.
    pub(crate) worker_claim_expires_at_unix_nanos: i64,
    /// Linked Saved Function path to the identity of the snapshot that
    /// acceptance retained for it.
    pub(crate) linked_snapshots: HashMap<String, String>,
}

/// The cap on Program output the Run Owner has applied but not yet recorded.
///
/// While the owner holds more than this many unrecorded output bytes, Program
/// requests answer only once the chunk that carries them commits, so the cap
/// bounds the loss set and the owner's memory without slowing the admission
/// burst. Set at process bootstrap; the default is 8 MiB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppliedOutputCap {
    bytes: usize,
}

impl AppliedOutputCap {
    /// The default cap, 8 MiB.
    pub const DEFAULT_BYTES: usize = 8 * 1024 * 1024;

    /// Accepts a cap of at least twice the chunk bytes cap, so one full chunk
    /// in flight and one being built never exceed it on their own. A single
    /// Event larger than the chunk bytes cap commits alone and can exceed the
    /// cap by itself; the cap then holds every request until it is recorded.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] below `2 ×` [`CHUNK_BYTES_CAP`].
    pub fn new(bytes: usize) -> Result<Self, Error> {
        if bytes < 2 * CHUNK_BYTES_CAP {
            return Err(Error::InvalidInput(format!(
                "applied output cap must be at least {} bytes, twice the chunk bytes cap",
                2 * CHUNK_BYTES_CAP
            )));
        }
        Ok(Self { bytes })
    }

    /// The cap in bytes.
    #[must_use]
    pub const fn bytes(self) -> usize {
        self.bytes
    }
}

impl Default for AppliedOutputCap {
    fn default() -> Self {
        Self {
            bytes: Self::DEFAULT_BYTES,
        }
    }
}

/// The Fan-out Limit: how many Invocations of one Run may be `dispatching`
/// at once.
///
/// The Run Owner decides each dispatch start against its in-memory count. A
/// ready Invocation above the limit stays `pending`, with no Event and no
/// required action, and starts its dispatch in readiness order when an
/// earlier one ends; waits for an account or an approval hold no slot.
/// Deployment-wide, set at process bootstrap; the default is 32.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FanOutLimit {
    invocations: usize,
}

impl FanOutLimit {
    /// The default limit, 32 Invocations.
    pub const DEFAULT_INVOCATIONS: usize = 32;

    /// Accepts a limit of at least one Invocation: at zero no dispatch could
    /// ever start.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for zero.
    pub fn new(invocations: usize) -> Result<Self, Error> {
        if invocations == 0 {
            return Err(Error::InvalidInput(
                "fan-out limit must be at least one Invocation".to_string(),
            ));
        }
        Ok(Self { invocations })
    }

    /// The limit in Invocations.
    #[must_use]
    pub const fn invocations(self) -> usize {
        self.invocations
    }
}

impl Default for FanOutLimit {
    fn default() -> Self {
        Self {
            invocations: Self::DEFAULT_INVOCATIONS,
        }
    }
}

/// The Shutdown Drain: how long a process that is shutting down lets its live
/// Runs end on their own before it interrupts the rest.
///
/// During the drain the process refuses new Runs and its Run Owners keep
/// working. Once it is out, every owner still live is interrupted and given
/// the commit grace to record that; a Run whose owner records nothing keeps
/// its lease, and the next process's expired-lease sweep ends it. Set at
/// process bootstrap; the default is 30 seconds, and zero interrupts at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownDrain {
    duration: Duration,
}

impl ShutdownDrain {
    /// The default drain, 30 seconds.
    pub const DEFAULT_DURATION: Duration = Duration::from_secs(30);

    /// A drain of `duration`; zero interrupts every live Run at once.
    #[must_use]
    pub const fn new(duration: Duration) -> Self {
        Self { duration }
    }

    /// How long the drain lasts.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.duration
    }
}

impl Default for ShutdownDrain {
    fn default() -> Self {
        Self::new(Self::DEFAULT_DURATION)
    }
}

/// The limits one deployment sets for Program execution: the V8 isolate
/// heaps, the Run Owner's Applied cap and its Fan-out Limit, and the
/// Shutdown Drain. Resolved once at process bootstrap and handed to the Run
/// module whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RuntimeLimits {
    isolate: crate::IsolateLimits,
    applied_output_cap: AppliedOutputCap,
    fan_out_limit: FanOutLimit,
    shutdown_drain: ShutdownDrain,
}

impl RuntimeLimits {
    /// Composes the limits bootstrap resolved from the `[runtime]` section.
    #[must_use]
    pub const fn new(
        isolate: crate::IsolateLimits,
        applied_output_cap: AppliedOutputCap,
        fan_out_limit: FanOutLimit,
        shutdown_drain: ShutdownDrain,
    ) -> Self {
        Self {
            isolate,
            applied_output_cap,
            fan_out_limit,
            shutdown_drain,
        }
    }

    /// How long a shutting-down process lets its live Runs end on their own.
    #[must_use]
    pub const fn shutdown_drain(&self) -> ShutdownDrain {
        self.shutdown_drain
    }

    /// The per-isolate heap budgets the compiler and the executor read.
    #[must_use]
    pub const fn isolate(&self) -> crate::IsolateLimits {
        self.isolate
    }

    /// The Run Owner's Applied cap on unrecorded Program output.
    #[must_use]
    pub const fn applied_output_cap(&self) -> AppliedOutputCap {
        self.applied_output_cap
    }

    /// The Fan-out Limit the Run Owner decides dispatch starts against.
    #[must_use]
    pub const fn fan_out_limit(&self) -> FanOutLimit {
        self.fan_out_limit
    }
}
