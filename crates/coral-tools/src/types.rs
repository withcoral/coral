use std::fmt;

use coral_catalog::{AuthoredToolKind, ToolAccessState};
use coral_workspace::{AuthenticatedPrincipal, Principal};
use coral_workspace::{TraceId, WorkspaceId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Error;
use crate::shared_tool::{SharedToolId, SharedToolPath};

/// Maximum UTF-8 byte length of one Private or Shared Tool Path.
pub const AUTHORED_TOOL_PATH_MAX_BYTES: usize = 256;
/// Maximum number of segments in one Private or Shared Tool Path.
pub const AUTHORED_TOOL_PATH_MAX_SEGMENTS: usize = 16;
/// Maximum UTF-8 byte length of one explicit Authored Tool description.
pub const AUTHORED_TOOL_DESCRIPTION_MAX_BYTES: usize = 4 * 1024;
/// The deadline for one save or Run compilation, the Program Authoring Limit
/// production composes. Each authoring Module receives its own value, so tests
/// can relax it.
pub const COMPILATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
/// Maximum UTF-8 byte length of one current Authored Tool source.
pub const AUTHORED_TOOL_SOURCE_MAX_BYTES: usize = 64 * 1024;
/// Maximum UTF-8 byte length of one emitted Authored Tool declaration.
pub const AUTHORED_TOOL_DECLARATION_MAX_BYTES: usize = 256 * 1024;
/// Maximum number of advisory diagnostics retained by one accepted save.
pub const AUTHORED_TOOL_DIAGNOSTIC_MAX_COUNT: usize = 100;
/// Maximum UTF-8 byte length retained for one advisory diagnostic message.
pub const AUTHORED_TOOL_DIAGNOSTIC_MESSAGE_MAX_BYTES: usize = 4 * 1024;

const PRIVATE_TOOL_DECLARATION_FIELDS_FORMAT: &str = "private-tool-declaration-fields-v1";
const AUTHORED_TOOL_DECLARATION_FIELD_MAX_COUNT: usize = 4_096;
const AUTHORED_TOOL_DECLARATION_FIELD_TEXT_MAX_BYTES: usize = 16 * 1_024;
const AUTHORED_TOOL_DECLARATION_FIELDS_MAX_BYTES: usize = 2 * 1_024 * 1_024;

/// Stable Coral-generated identity retained across replacements at one current path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PrivateToolId(Uuid);

impl PrivateToolId {
    pub(crate) fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    /// Parses a Private Tool identity at an Adapter boundary.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] unless the value is a canonical UUID.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let id = Uuid::parse_str(value)
            .map_err(|_error| Error::InvalidInput("Private Tool id must be a UUID".to_string()))?;
        if id.hyphenated().to_string() != value {
            return Err(Error::InvalidInput(
                "Private Tool id must use canonical lowercase UUID form".to_string(),
            ));
        }
        Ok(Self(id))
    }
}

impl PrivateToolId {
    /// The code Execution Binding of this Private Tool's definition at
    /// `revision`.
    fn execution_binding(&self, revision: u64) -> coral_catalog::CodeExecutionBinding {
        coral_catalog::CodeExecutionBinding::new(self.to_string(), revision)
    }

    /// The Private Tool's Tool Key in the Private Provider: its stable
    /// identity, which an exact rule names.
    ///
    /// # Panics
    ///
    /// Never: a canonical UUID is always a valid Tool Key.
    #[must_use]
    pub fn tool_key(&self) -> coral_catalog::ToolKey {
        coral_catalog::ToolKey::parse(&self.to_string())
            .expect("a canonical UUID is a valid Tool Key")
    }
}

impl fmt::Display for PrivateToolId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0.hyphenated())
    }
}

/// A UUID Coral generated, or one parsed in its canonical form: it holds
/// no text.
impl coral_telemetry::safe::Identifier for PrivateToolId {}

/// Principal-owned dotted JavaScript path relative to `coral.private`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PrivateToolPath(String);

impl PrivateToolPath {
    /// Validates the fixed path bounds and safe JavaScript segments.
    ///
    /// # Errors
    ///
    /// Rejects empty paths, more than sixteen segments, more than 256 UTF-8
    /// bytes, and invalid or reserved JavaScript identifier segments.
    pub fn parse(value: &str) -> Result<Self, Error> {
        authored_tool_path(value, "Private Tool Path").map(Self)
    }

    /// Returns the complete dotted path relative to `coral.private`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns validated segments for the compiler's linked source input.
    #[must_use]
    pub fn segments(&self) -> Vec<String> {
        self.0.split('.').map(ToString::to_string).collect()
    }
}

/// The stable identity of one Authored Tool, which names its kind. It is the
/// Tool Key of the Tool in its kind's Provider.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AuthoredToolId {
    /// A Principal's own Private Tool.
    Private(PrivateToolId),
    /// The Workspace's Shared Tool.
    Shared(SharedToolId),
}

impl AuthoredToolId {
    /// Parses the identity of a Tool of `kind`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] unless the value is a canonical UUID.
    pub fn parse(kind: AuthoredToolKind, value: &str) -> Result<Self, Error> {
        match kind {
            AuthoredToolKind::Private => PrivateToolId::parse(value).map(Self::Private),
            AuthoredToolKind::Shared => SharedToolId::parse(value).map(Self::Shared),
        }
    }

    /// The kind of Tool the identity names.
    #[must_use]
    pub const fn kind(&self) -> AuthoredToolKind {
        match self {
            Self::Private(_) => AuthoredToolKind::Private,
            Self::Shared(_) => AuthoredToolKind::Shared,
        }
    }

    /// The Tool Key in the kind's Provider, which an exact rule names.
    #[must_use]
    pub fn tool_key(&self) -> coral_catalog::ToolKey {
        match self {
            Self::Private(id) => id.tool_key(),
            Self::Shared(id) => id.tool_key(),
        }
    }

    /// The identity when it names a Private Tool.
    #[must_use]
    pub const fn private(&self) -> Option<&PrivateToolId> {
        match self {
            Self::Private(id) => Some(id),
            Self::Shared(_) => None,
        }
    }

    /// The identity when it names a Shared Tool.
    #[must_use]
    pub const fn shared(&self) -> Option<&SharedToolId> {
        match self {
            Self::Private(_) => None,
            Self::Shared(id) => Some(id),
        }
    }
}

impl fmt::Display for AuthoredToolId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Private(id) => id.fmt(formatter),
            Self::Shared(id) => id.fmt(formatter),
        }
    }
}

impl From<PrivateToolId> for AuthoredToolId {
    fn from(id: PrivateToolId) -> Self {
        Self::Private(id)
    }
}

impl From<SharedToolId> for AuthoredToolId {
    fn from(id: SharedToolId) -> Self {
        Self::Shared(id)
    }
}

/// The path of one Authored Tool under its kind's Call Root: what a source
/// names to link it, and what Universal Search finds it by.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AuthoredToolPath {
    /// The invoking Principal's own Private Tool, under `coral.private`.
    Private(PrivateToolPath),
    /// The Workspace's Shared Tool, under `coral.shared`.
    Shared(SharedToolPath),
}

impl AuthoredToolPath {
    /// Parses the path of a Tool of `kind`, relative to the kind's Call
    /// Root.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for a path outside the authoring
    /// bounds.
    pub fn parse(kind: AuthoredToolKind, relative_path: &str) -> Result<Self, Error> {
        match kind {
            AuthoredToolKind::Private => PrivateToolPath::parse(relative_path).map(Self::Private),
            AuthoredToolKind::Shared => SharedToolPath::parse(relative_path).map(Self::Shared),
        }
    }

    /// The kind of Tool the path names.
    #[must_use]
    pub const fn kind(&self) -> AuthoredToolKind {
        match self {
            Self::Private(_) => AuthoredToolKind::Private,
            Self::Shared(_) => AuthoredToolKind::Shared,
        }
    }

    /// The dotted path under the kind's Call Root.
    #[must_use]
    pub fn relative_path(&self) -> &str {
        match self {
            Self::Private(path) => path.as_str(),
            Self::Shared(path) => path.as_str(),
        }
    }

    /// The validated segments under the kind's Call Root, for the
    /// compiler's linked source input.
    #[must_use]
    pub fn segments(&self) -> Vec<String> {
        match self {
            Self::Private(path) => path.segments(),
            Self::Shared(path) => path.segments(),
        }
    }

    /// The complete Call Path, such as `coral.shared.mail.remind`.
    #[must_use]
    pub fn call_path(&self) -> String {
        self.kind().call_path(self.relative_path())
    }

    /// How a Run's admission refusal and a stack frame name the Tool's
    /// source, such as `Shared Tool coral.shared.mail.remind`. An authoring
    /// refusal names the kind itself, and then the Call Path alone.
    #[must_use]
    pub fn source_name(&self) -> String {
        self.kind().source_name(self.relative_path())
    }
}

/// Validates the dotted path of a Private or Shared Tool, which `term`
/// names in the refusal: 1 through [`AUTHORED_TOOL_PATH_MAX_SEGMENTS`]
/// non-reserved safe JavaScript identifier segments, in at most
/// [`AUTHORED_TOOL_PATH_MAX_BYTES`] UTF-8 bytes.
pub(crate) fn authored_tool_path(value: &str, term: &str) -> Result<String, Error> {
    let segments = value.split('.').collect::<Vec<_>>();
    if value.is_empty()
        || value.len() > AUTHORED_TOOL_PATH_MAX_BYTES
        || segments.len() > AUTHORED_TOOL_PATH_MAX_SEGMENTS
        || segments.iter().any(|segment| !safe_path_segment(segment))
    {
        return Err(Error::InvalidInput(format!(
            "{term} must contain 1 through {AUTHORED_TOOL_PATH_MAX_SEGMENTS} non-reserved safe JavaScript identifier segments and be at most {AUTHORED_TOOL_PATH_MAX_BYTES} UTF-8 bytes"
        )));
    }
    Ok(value.to_string())
}

fn safe_path_segment(value: &str) -> bool {
    let mut characters = value.chars();
    let valid_first = characters.next().is_some_and(|character| {
        matches!(character, '_' | '$') || unicode_id_start::is_id_start(character)
    });
    valid_first
        && characters.all(|character| {
            matches!(character, '_' | '$' | '\u{200c}' | '\u{200d}')
                || unicode_id_start::is_id_continue(character)
        })
        && !coral_catalog::call_path_segment_is_reserved(value)
}

/// Explicit safe human-facing description of one Authored Tool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoredToolDescription(String);

impl AuthoredToolDescription {
    /// Validates an explicit bounded description.
    ///
    /// # Errors
    ///
    /// Rejects blank text, surrounding whitespace, null or control characters,
    /// and values larger than the fixed UTF-8 byte limit.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.is_empty()
            || value.trim() != value
            || value.len() > AUTHORED_TOOL_DESCRIPTION_MAX_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(Error::InvalidInput(format!(
                "Tool description must be non-blank, have no surrounding whitespace or control characters, and be at most {AUTHORED_TOOL_DESCRIPTION_MAX_BYTES} UTF-8 bytes"
            )));
        }
        Ok(Self(value.to_string()))
    }

    /// Returns the exact accepted description.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Exact authored no-import TypeScript module source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoredToolSource(String);

impl AuthoredToolSource {
    /// Applies the fixed source limit before authoring compilation.
    ///
    /// # Errors
    ///
    /// Rejects empty source, null bytes, and source larger than 64 KiB.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.is_empty() || value.contains('\0') || value.len() > AUTHORED_TOOL_SOURCE_MAX_BYTES
        {
            return Err(Error::InvalidInput(format!(
                "Tool source must be non-empty, contain no null byte, and be at most {AUTHORED_TOOL_SOURCE_MAX_BYTES} UTF-8 bytes"
            )));
        }
        Ok(Self(value.to_string()))
    }

    /// Returns the exact authored TypeScript source.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Exact self-contained declaration emitted by the pinned compiler profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoredToolDeclaration(String);

impl AuthoredToolDeclaration {
    /// The declaration the compiler emitted, or the reason Coral does not
    /// keep it. The reason becomes the text of an authoring failure, whose
    /// caller names the kind of Tool and its path, so it says what the
    /// author can change.
    pub(crate) fn from_compiler(value: &str) -> Result<Self, String> {
        if value.is_empty() {
            return Err("TypeScript emitted no Tool declaration".to_string());
        }
        if value.len() > AUTHORED_TOOL_DECLARATION_MAX_BYTES {
            return Err(format!(
                "the Tool declaration exceeds {AUTHORED_TOOL_DECLARATION_MAX_BYTES} bytes. Return fewer fields, or annotate the return type."
            ));
        }
        Ok(Self(value.to_string()))
    }

    pub(crate) fn from_persisted(value: &str) -> Result<Self, Error> {
        Self::from_compiler(value).map_err(|reason| {
            Error::CorruptData(format!("authored Tool has invalid declaration: {reason}"))
        })
    }

    /// Returns the exact emitted TypeScript declaration.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One callable-rooted declaration field prepared for Universal Search.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoredToolDeclarationField {
    path: coral_catalog::DeclarationPath,
    text: String,
}

impl AuthoredToolDeclarationField {
    fn from_compiler(field: &coral_v8::AuthoredToolDeclarationField) -> Result<Self, ()> {
        Ok(Self {
            path: coral_catalog::DeclarationPath::parse(field.path()).map_err(|_error| ())?,
            text: field.text().to_string(),
        })
    }

    /// Returns a readable JavaScript path rooted at `input` or `result`.
    #[must_use]
    pub fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Returns names, documentation, and type evidence for this path.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }
}

/// Non-blocking declaration-field projection retained with one accepted save.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthoredToolDeclarationFieldProjection {
    /// Searchable declaration fields were prepared successfully.
    Ready(Vec<AuthoredToolDeclarationField>),
    /// A stable private failure reason was retained without rejecting authoring.
    Failed(String),
}

impl AuthoredToolDeclarationFieldProjection {
    pub(crate) fn from_compiler(
        projection: Result<&[coral_v8::AuthoredToolDeclarationField], &str>,
    ) -> Self {
        let Ok(fields) = projection else {
            return Self::Failed("declaration_field_projection_failed".to_string());
        };
        let Ok(mut fields) = fields
            .iter()
            .map(AuthoredToolDeclarationField::from_compiler)
            .collect::<Result<Vec<_>, _>>()
        else {
            return Self::Failed("declaration_field_projection_failed".to_string());
        };
        fields.sort_by(|left, right| left.path.cmp(&right.path));
        if !valid_declaration_fields(&fields) {
            return Self::Failed("declaration_field_projection_too_large".to_string());
        }
        Self::Ready(fields)
    }

    pub(crate) const fn status(&self) -> &'static str {
        match self {
            Self::Ready(_) => "ready",
            Self::Failed(_) => "failed",
        }
    }

    pub(crate) fn fields(&self) -> Option<&[AuthoredToolDeclarationField]> {
        match self {
            Self::Ready(fields) => Some(fields),
            Self::Failed(_) => None,
        }
    }

    pub(crate) fn failure_reason(&self) -> Option<&str> {
        match self {
            Self::Ready(_) => None,
            Self::Failed(reason) => Some(reason),
        }
    }

    pub(crate) const fn format() -> &'static str {
        PRIVATE_TOOL_DECLARATION_FIELDS_FORMAT
    }

    pub(crate) fn from_storage(
        format: &str,
        status: &str,
        fields_json: Option<&str>,
        failure_reason: Option<&str>,
    ) -> Result<Self, Error> {
        if format != PRIVATE_TOOL_DECLARATION_FIELDS_FORMAT {
            return Err(Error::CorruptData(
                "authored Tool declaration fields have an incompatible format".to_string(),
            ));
        }
        match (status, fields_json, failure_reason) {
            ("ready", Some(json), None) => {
                let fields: Vec<AuthoredToolDeclarationField> = serde_json::from_str(json)
                    .map_err(|_error| {
                        Error::CorruptData(
                            "authored Tool declaration fields are not valid JSON".to_string(),
                        )
                    })?;
                if !valid_declaration_fields(&fields) {
                    return Err(Error::CorruptData(
                        "authored Tool declaration fields exceed their bounds".to_string(),
                    ));
                }
                Ok(Self::Ready(fields))
            }
            ("failed", None, Some(reason))
                if matches!(
                    reason,
                    "declaration_field_projection_failed"
                        | "declaration_field_projection_too_large"
                ) =>
            {
                Ok(Self::Failed(reason.to_string()))
            }
            _ => Err(Error::CorruptData(
                "authored Tool has an invalid declaration-field projection outcome".to_string(),
            )),
        }
    }
}

fn valid_declaration_fields(fields: &[AuthoredToolDeclarationField]) -> bool {
    if fields.len() > AUTHORED_TOOL_DECLARATION_FIELD_MAX_COUNT {
        return false;
    }
    let mut previous = None;
    let mut total = 2usize;
    for field in fields {
        if field.text.is_empty()
            || field.text.len() > AUTHORED_TOOL_DECLARATION_FIELD_TEXT_MAX_BYTES
            || previous.is_some_and(|value: &coral_catalog::DeclarationPath| value >= &field.path)
        {
            return false;
        }
        total = total
            .saturating_add(field.path.as_str().len())
            .saturating_add(field.text.len())
            .saturating_add(32);
        previous = Some(&field.path);
    }
    total <= AUTHORED_TOOL_DECLARATION_FIELDS_MAX_BYTES
}

/// Stable category of one advisory TypeScript authoring diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthoredToolDiagnosticCategory {
    /// Informational compiler message.
    Message,
    /// Non-blocking warning.
    Warning,
    /// Ordinary strict semantic error retained without rejecting the save.
    Error,
    /// Non-blocking authoring suggestion.
    Suggestion,
}

/// One bounded advisory diagnostic retained with an accepted Authored Tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoredToolDiagnostic {
    category: AuthoredToolDiagnosticCategory,
    code: u64,
    message: String,
    line: Option<u64>,
    column: Option<u64>,
}

impl AuthoredToolDiagnostic {
    pub(crate) fn from_compiler(diagnostic: &coral_v8::CompilerDiagnostic) -> Self {
        let category = match diagnostic.category() {
            coral_v8::DiagnosticCategory::Message => AuthoredToolDiagnosticCategory::Message,
            coral_v8::DiagnosticCategory::Warning => AuthoredToolDiagnosticCategory::Warning,
            coral_v8::DiagnosticCategory::Error => AuthoredToolDiagnosticCategory::Error,
            coral_v8::DiagnosticCategory::Suggestion => AuthoredToolDiagnosticCategory::Suggestion,
        };
        Self {
            category,
            code: diagnostic.code(),
            message: truncate_utf8(
                diagnostic.message(),
                AUTHORED_TOOL_DIAGNOSTIC_MESSAGE_MAX_BYTES,
            ),
            line: diagnostic.line(),
            column: diagnostic.column(),
        }
    }

    /// Returns the diagnostic category.
    #[must_use]
    pub const fn category(&self) -> AuthoredToolDiagnosticCategory {
        self.category
    }

    /// Returns the official TypeScript diagnostic code.
    #[must_use]
    pub const fn code(&self) -> u64 {
        self.code
    }

    /// Returns the bounded safe compiler message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the one-based authored line when available.
    #[must_use]
    pub const fn line(&self) -> Option<u64> {
        self.line
    }

    /// Returns the one-based authored column when available.
    #[must_use]
    pub const fn column(&self) -> Option<u64> {
        self.column
    }
}

fn truncate_utf8(value: &str, maximum_bytes: usize) -> String {
    value
        .get(..value.floor_char_boundary(maximum_bytes))
        .expect("floor_char_boundary returns a valid UTF-8 boundary")
        .to_owned()
}

/// Complete authoring input for one atomic save: create or replace by
/// default, or only one of them through [`Self::create_only`] and
/// [`Self::replace_only`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateToolSave {
    path: PrivateToolPath,
    description: AuthoredToolDescription,
    source: AuthoredToolSource,
    intent: SaveIntent,
    declared_default: Option<ToolAccessState>,
}

/// What a save requires of the Private Tool currently at its path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SaveIntent {
    CreateOrReplace,
    Create,
    Replace { expected_revision: Option<u64> },
}

impl PrivateToolSave {
    /// Builds already-validated authoring input that creates or replaces.
    #[must_use]
    pub const fn new(
        path: PrivateToolPath,
        description: AuthoredToolDescription,
        source: AuthoredToolSource,
    ) -> Self {
        Self {
            path,
            description,
            source,
            intent: SaveIntent::CreateOrReplace,
            declared_default: None,
        }
    }

    /// Declares `declared`, if any, as the Private Tool's default Tool
    /// Access State, which applies when no exact rule matches. A save that
    /// declares none creates an `enabled` Private Tool, or keeps the default
    /// of the Private Tool it replaces.
    #[must_use]
    pub const fn declaring_default(mut self, declared: Option<ToolAccessState>) -> Self {
        self.declared_default = declared;
        self
    }

    /// Saves only when no Private Tool is at the path; otherwise the save fails
    /// with [`Error::PrivateToolExists`].
    #[must_use]
    pub const fn create_only(mut self) -> Self {
        self.intent = SaveIntent::Create;
        self
    }

    /// Saves only over a Private Tool at the path, and, with
    /// `expected_revision`, only while that Private Tool is at that revision.
    /// Otherwise the save fails with [`Error::NotFound`] or
    /// [`Error::RevisionConflict`].
    #[must_use]
    pub const fn replace_only(mut self, expected_revision: Option<u64>) -> Self {
        self.intent = SaveIntent::Replace { expected_revision };
        self
    }

    /// Whether the save requires anything of the Private Tool at its path.
    pub(crate) const fn has_precondition(&self) -> bool {
        !matches!(self.intent, SaveIntent::CreateOrReplace)
    }

    /// Checks the intent against the revision of the Private Tool at the path:
    /// first from a snapshot, so a refused save never compiles, then under
    /// the save's hold, which decides.
    pub(crate) const fn admits(&self, current_revision: Option<u64>) -> Result<(), Error> {
        match (self.intent, current_revision) {
            (SaveIntent::Create, Some(_)) => Err(Error::PrivateToolExists),
            (SaveIntent::Replace { .. }, None) => Err(Error::NotFound(AuthoredToolKind::Private)),
            (
                SaveIntent::Replace {
                    expected_revision: Some(expected),
                },
                Some(current),
            ) if expected != current => Err(Error::RevisionConflict {
                kind: AuthoredToolKind::Private,
                current,
            }),
            _ => Ok(()),
        }
    }

    /// Returns the path relative to `coral.private` that it saves.
    #[must_use]
    pub const fn path(&self) -> &PrivateToolPath {
        &self.path
    }

    pub(crate) const fn description(&self) -> &AuthoredToolDescription {
        &self.description
    }

    pub(crate) const fn source(&self) -> &AuthoredToolSource {
        &self.source
    }

    /// The default the saved Private Tool takes: the declared one, or else
    /// `previous`, the replaced Private Tool's, or `enabled` for a new one.
    pub(crate) fn default_access_state(
        &self,
        previous: Option<ToolAccessState>,
    ) -> ToolAccessState {
        self.declared_default
            .or(previous)
            .unwrap_or(Self::NEW_TOOL_DEFAULT)
    }

    /// The default of a new Private Tool whose save declares none.
    pub(crate) const NEW_TOOL_DEFAULT: ToolAccessState = ToolAccessState::Enabled;
}

/// Authenticated, correlated occurrence evidence for one authoring change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateToolChangeContext {
    actor: AuthenticatedPrincipal,
    occurred_at_unix_nanos: i64,
    trace_id: TraceId,
}

impl PrivateToolChangeContext {
    /// Builds Adapter-supplied authoring context.
    #[must_use]
    pub const fn new(
        actor: AuthenticatedPrincipal,
        occurred_at_unix_nanos: i64,
        trace_id: TraceId,
    ) -> Self {
        Self {
            actor,
            occurred_at_unix_nanos,
            trace_id,
        }
    }

    /// Returns the accountable owning Principal.
    #[must_use]
    pub const fn actor(&self) -> &AuthenticatedPrincipal {
        &self.actor
    }

    /// Returns occurrence metadata; it is not a stale-write guard.
    #[must_use]
    pub const fn occurred_at_unix_nanos(&self) -> i64 {
        self.occurred_at_unix_nanos
    }

    /// Returns safe trace correlation evidence.
    #[must_use]
    pub const fn trace_id(&self) -> &TraceId {
        &self.trace_id
    }
}

/// One Principal's current accepted Private Tool definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateTool {
    id: PrivateToolId,
    workspace_id: WorkspaceId,
    principal: Principal,
    path: PrivateToolPath,
    description: AuthoredToolDescription,
    source: AuthoredToolSource,
    declaration: AuthoredToolDeclaration,
    declaration_field_projection: AuthoredToolDeclarationFieldProjection,
    diagnostics: Vec<AuthoredToolDiagnostic>,
    diagnostics_truncated: bool,
    saved_at_unix_nanos: i64,
    pub(crate) revision: u64,
    default_access_state: ToolAccessState,
}

impl PrivateTool {
    #[expect(
        clippy::too_many_arguments,
        reason = "the accepted current definition is assembled once at its persistence boundary"
    )]
    pub(crate) const fn new(
        id: PrivateToolId,
        workspace_id: WorkspaceId,
        principal: Principal,
        path: PrivateToolPath,
        description: AuthoredToolDescription,
        source: AuthoredToolSource,
        declaration: AuthoredToolDeclaration,
        declaration_field_projection: AuthoredToolDeclarationFieldProjection,
        diagnostics: Vec<AuthoredToolDiagnostic>,
        diagnostics_truncated: bool,
        saved_at_unix_nanos: i64,
        revision: u64,
        default_access_state: ToolAccessState,
    ) -> Self {
        Self {
            id,
            workspace_id,
            principal,
            path,
            description,
            source,
            declaration,
            declaration_field_projection,
            diagnostics,
            diagnostics_truncated,
            saved_at_unix_nanos,
            revision,
            default_access_state,
        }
    }

    /// Returns the stable identity retained across replacements at this path.
    #[must_use]
    pub const fn id(&self) -> &PrivateToolId {
        &self.id
    }

    /// Returns the Workspace confidentiality boundary.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    /// Returns the owning Principal.
    #[must_use]
    pub const fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Returns the stable current path.
    #[must_use]
    pub const fn path(&self) -> &PrivateToolPath {
        &self.path
    }

    /// Returns the exact explicit description.
    #[must_use]
    pub const fn description(&self) -> &AuthoredToolDescription {
        &self.description
    }

    /// Returns the exact authored source.
    #[must_use]
    pub const fn source(&self) -> &AuthoredToolSource {
        &self.source
    }

    /// Returns the exact self-contained declaration.
    #[must_use]
    pub const fn declaration(&self) -> &AuthoredToolDeclaration {
        &self.declaration
    }

    /// Returns the non-blocking declaration-field projection outcome.
    #[must_use]
    pub const fn declaration_field_projection(&self) -> &AuthoredToolDeclarationFieldProjection {
        &self.declaration_field_projection
    }

    /// Returns bounded strict semantic diagnostics.
    #[must_use]
    pub fn diagnostics(&self) -> &[AuthoredToolDiagnostic] {
        &self.diagnostics
    }

    /// Returns whether additional compiler diagnostics were omitted by the bound.
    #[must_use]
    pub const fn diagnostics_truncated(&self) -> bool {
        self.diagnostics_truncated
    }

    /// Returns occurrence metadata from the latest accepted save.
    #[must_use]
    pub const fn saved_at_unix_nanos(&self) -> i64 {
        self.saved_at_unix_nanos
    }

    /// Returns the definition revision; each accepted save at this path adds one.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the Private Tool's Tool Access State when no exact rule
    /// matches it.
    #[must_use]
    pub const fn default_access_state(&self) -> ToolAccessState {
        self.default_access_state
    }

    /// Returns the code Execution Binding of this Private Tool: the
    /// definition Coral performs when it is called.
    #[must_use]
    pub fn execution_binding(&self) -> coral_catalog::CodeExecutionBinding {
        self.id.execution_binding(self.revision)
    }
}

/// Most summaries one list page returns.
pub const PRIVATE_TOOL_PAGE_MAX_SIZE: u32 = 100;

/// Source-free summary of one current Private Tool for browsing.
///
/// It contains no source, declaration, or diagnostics; exact reads load those.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateToolSummary {
    path: PrivateToolPath,
    description: AuthoredToolDescription,
    saved_at_unix_nanos: i64,
}

impl PrivateToolSummary {
    pub(crate) const fn new(
        path: PrivateToolPath,
        description: AuthoredToolDescription,
        saved_at_unix_nanos: i64,
    ) -> Self {
        Self {
            path,
            description,
            saved_at_unix_nanos,
        }
    }

    /// Returns the path relative to `coral.private`.
    #[must_use]
    pub const fn path(&self) -> &PrivateToolPath {
        &self.path
    }

    /// Returns the exact explicit description.
    #[must_use]
    pub const fn description(&self) -> &AuthoredToolDescription {
        &self.description
    }

    /// Returns occurrence metadata from the latest accepted save.
    #[must_use]
    pub const fn saved_at_unix_nanos(&self) -> i64 {
        self.saved_at_unix_nanos
    }
}

/// One bounded page of summaries in path order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateToolPage {
    summaries: Vec<PrivateToolSummary>,
    next_after: Option<PrivateToolPath>,
}

impl PrivateToolPage {
    pub(crate) const fn new(
        summaries: Vec<PrivateToolSummary>,
        next_after: Option<PrivateToolPath>,
    ) -> Self {
        Self {
            summaries,
            next_after,
        }
    }

    /// Returns this page's summaries in path order.
    #[must_use]
    pub fn summaries(&self) -> &[PrivateToolSummary] {
        &self.summaries
    }

    /// Returns the path to pass as `after` for the next page, when one exists.
    #[must_use]
    pub const fn next_after(&self) -> Option<&PrivateToolPath> {
        self.next_after.as_ref()
    }
}

/// A Private Tool whose policy a reader may read, as a policy listing shows
/// it: its Tool Key, its path, the Agent whose it is (`None` for the
/// reader's own), and its declared default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadablePrivateTool {
    key: coral_catalog::ToolKey,
    path: PrivateToolPath,
    agent: Option<coral_workspace::PrincipalId>,
    default_access_state: ToolAccessState,
}

impl ReadablePrivateTool {
    pub(crate) const fn new(
        key: coral_catalog::ToolKey,
        path: PrivateToolPath,
        agent: Option<coral_workspace::PrincipalId>,
        default_access_state: ToolAccessState,
    ) -> Self {
        Self {
            key,
            path,
            agent,
            default_access_state,
        }
    }

    /// Returns the Tool Key in the Private Provider.
    #[must_use]
    pub const fn key(&self) -> &coral_catalog::ToolKey {
        &self.key
    }

    /// Returns the path relative to `coral.private`.
    #[must_use]
    pub const fn path(&self) -> &PrivateToolPath {
        &self.path
    }

    /// Returns the Agent whose Private Tool this is; `None` for the
    /// reader's own.
    #[must_use]
    pub const fn agent(&self) -> Option<&coral_workspace::PrincipalId> {
        self.agent.as_ref()
    }

    /// Returns the default its author declared.
    #[must_use]
    pub const fn default_access_state(&self) -> ToolAccessState {
        self.default_access_state
    }
}

/// Whose Private Tools a reader of their policy sees, beside its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivateToolScope {
    /// Only the reader's own.
    Own,
    /// The reader's own and every Agent's, as a Workspace Owner reads them.
    OwnAndAgents,
}

/// Accepted Private Tool lifecycle action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleAction {
    /// A previously absent current path was created.
    Created,
    /// An existing current path was atomically replaced.
    Replaced,
    /// A current path was deleted.
    Deleted,
}

impl LifecycleAction {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Created => "private_tool.created",
            Self::Replaced => "private_tool.replaced",
            Self::Deleted => "private_tool.deleted",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "private_tool.created" => Ok(Self::Created),
            "private_tool.replaced" => Ok(Self::Replaced),
            "private_tool.deleted" => Ok(Self::Deleted),
            _ => Err(Error::CorruptData(format!(
                "Private Tool lifecycle Event has unrecognized action '{value}'"
            ))),
        }
    }
}

/// Accepted result carried by a Private Tool lifecycle Event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleOutcome {
    /// The authoring change and Event committed together.
    Accepted,
}

impl LifecycleOutcome {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "accepted" => Ok(Self::Accepted),
            _ => Err(Error::CorruptData(format!(
                "Private Tool lifecycle Event has unrecognized outcome '{value}'"
            ))),
        }
    }
}

/// Safe immutable evidence of one accepted Private Tool authoring change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdministrativeLifecycleEvent {
    workspace_id: WorkspaceId,
    actor: Principal,
    action: LifecycleAction,
    outcome: LifecycleOutcome,
    target_id: PrivateToolId,
    target_path: PrivateToolPath,
    changed_field_paths: Vec<String>,
    occurred_at_unix_nanos: i64,
    trace_id: TraceId,
}

impl AdministrativeLifecycleEvent {
    #[expect(
        clippy::too_many_arguments,
        reason = "the immutable Event is decoded once at its persistence boundary"
    )]
    pub(crate) const fn new(
        workspace_id: WorkspaceId,
        actor: Principal,
        action: LifecycleAction,
        outcome: LifecycleOutcome,
        target_id: PrivateToolId,
        target_path: PrivateToolPath,
        changed_field_paths: Vec<String>,
        occurred_at_unix_nanos: i64,
        trace_id: TraceId,
    ) -> Self {
        Self {
            workspace_id,
            actor,
            action,
            outcome,
            target_id,
            target_path,
            changed_field_paths,
            occurred_at_unix_nanos,
            trace_id,
        }
    }

    /// Returns the Workspace domain that owns this Event.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    /// Returns the accountable Principal actor.
    #[must_use]
    pub const fn actor(&self) -> &Principal {
        &self.actor
    }

    /// Returns the accepted action.
    #[must_use]
    pub const fn action(&self) -> LifecycleAction {
        self.action
    }

    /// Returns the accepted outcome.
    #[must_use]
    pub const fn outcome(&self) -> LifecycleOutcome {
        self.outcome
    }

    /// Returns the stable Private Tool target identity.
    #[must_use]
    pub const fn target_id(&self) -> &PrivateToolId {
        &self.target_id
    }

    /// Returns the safe target path without source or description.
    #[must_use]
    pub const fn target_path(&self) -> &PrivateToolPath {
        &self.target_path
    }

    /// Returns safe changed field paths without values.
    #[must_use]
    pub fn changed_field_paths(&self) -> &[String] {
        &self.changed_field_paths
    }

    /// Returns occurrence metadata for the committed change.
    #[must_use]
    pub const fn occurred_at_unix_nanos(&self) -> i64 {
        self.occurred_at_unix_nanos
    }

    /// Returns safe trace correlation evidence.
    #[must_use]
    pub const fn trace_id(&self) -> &TraceId {
        &self.trace_id
    }
}
