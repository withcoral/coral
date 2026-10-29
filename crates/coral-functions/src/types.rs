use std::fmt;

use coral_workspace::{AuthenticatedPrincipal, Principal};
use coral_workspace::{TraceId, WorkspaceId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Error;

/// Maximum UTF-8 byte length of one Saved Function Path.
pub const SAVED_FUNCTION_PATH_MAX_BYTES: usize = 256;
/// Maximum number of segments in one Saved Function Path.
pub const SAVED_FUNCTION_PATH_MAX_SEGMENTS: usize = 16;
/// Maximum UTF-8 byte length of one explicit Saved Function description.
pub const SAVED_FUNCTION_DESCRIPTION_MAX_BYTES: usize = 4 * 1024;
/// Maximum UTF-8 byte length of one current Saved Function source.
pub const SAVED_FUNCTION_SOURCE_MAX_BYTES: usize = 64 * 1024;
/// Maximum UTF-8 byte length of one emitted Saved Function declaration.
pub const SAVED_FUNCTION_DECLARATION_MAX_BYTES: usize = 256 * 1024;
/// Maximum number of advisory diagnostics retained by one accepted save.
pub const SAVED_FUNCTION_DIAGNOSTIC_MAX_COUNT: usize = 100;
/// Maximum UTF-8 byte length retained for one advisory diagnostic message.
pub const SAVED_FUNCTION_DIAGNOSTIC_MESSAGE_MAX_BYTES: usize = 4 * 1024;

const SAVED_FUNCTION_DECLARATION_FIELDS_FORMAT: &str = "saved-function-declaration-fields-v1";
const SAVED_FUNCTION_DECLARATION_FIELD_MAX_COUNT: usize = 4_096;
const SAVED_FUNCTION_DECLARATION_FIELD_TEXT_MAX_BYTES: usize = 16 * 1_024;
const SAVED_FUNCTION_DECLARATION_FIELDS_MAX_BYTES: usize = 2 * 1_024 * 1_024;

/// Stable Coral-generated identity retained across replacements at one current path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SavedFunctionId(Uuid);

impl SavedFunctionId {
    pub(crate) fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    /// Parses a Saved Function identity at an Adapter boundary.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] unless the value is a canonical UUID.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let id = Uuid::parse_str(value).map_err(|_error| {
            Error::InvalidInput("Saved Function id must be a UUID".to_string())
        })?;
        if id.hyphenated().to_string() != value {
            return Err(Error::InvalidInput(
                "Saved Function id must use canonical lowercase UUID form".to_string(),
            ));
        }
        Ok(Self(id))
    }
}

impl fmt::Display for SavedFunctionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0.hyphenated())
    }
}

/// A UUID Coral generated, or one parsed in its canonical form: it holds
/// no text.
impl coral_telemetry::safe::Identifier for SavedFunctionId {}

/// Principal-owned dotted JavaScript path relative to `coral.functions`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SavedFunctionPath(String);

impl SavedFunctionPath {
    /// Validates the fixed path bounds and safe JavaScript segments.
    ///
    /// # Errors
    ///
    /// Rejects empty paths, more than sixteen segments, more than 256 UTF-8
    /// bytes, and invalid or reserved JavaScript identifier segments.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let segments = value.split('.').collect::<Vec<_>>();
        if value.is_empty()
            || value.len() > SAVED_FUNCTION_PATH_MAX_BYTES
            || segments.len() > SAVED_FUNCTION_PATH_MAX_SEGMENTS
            || segments.iter().any(|segment| !safe_path_segment(segment))
        {
            return Err(Error::InvalidInput(format!(
                "Saved Function Path must contain 1 through {SAVED_FUNCTION_PATH_MAX_SEGMENTS} non-reserved safe JavaScript identifier segments and be at most {SAVED_FUNCTION_PATH_MAX_BYTES} UTF-8 bytes"
            )));
        }
        Ok(Self(value.to_string()))
    }

    /// Returns the complete dotted path relative to `coral.functions`.
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

/// Explicit safe human-facing description of one Saved Function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedFunctionDescription(String);

impl SavedFunctionDescription {
    /// Validates an explicit bounded description.
    ///
    /// # Errors
    ///
    /// Rejects blank text, surrounding whitespace, null or control characters,
    /// and values larger than the fixed UTF-8 byte limit.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.is_empty()
            || value.trim() != value
            || value.len() > SAVED_FUNCTION_DESCRIPTION_MAX_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(Error::InvalidInput(format!(
                "Saved Function description must be non-blank, have no surrounding whitespace or control characters, and be at most {SAVED_FUNCTION_DESCRIPTION_MAX_BYTES} UTF-8 bytes"
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
pub struct SavedFunctionSource(String);

impl SavedFunctionSource {
    /// Applies the fixed source limit before authoring compilation.
    ///
    /// # Errors
    ///
    /// Rejects empty source, null bytes, and source larger than 64 KiB.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.is_empty() || value.contains('\0') || value.len() > SAVED_FUNCTION_SOURCE_MAX_BYTES
        {
            return Err(Error::InvalidInput(format!(
                "Saved Function source must be non-empty, contain no null byte, and be at most {SAVED_FUNCTION_SOURCE_MAX_BYTES} UTF-8 bytes"
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
pub struct SavedFunctionDeclaration(String);

impl SavedFunctionDeclaration {
    pub(crate) fn from_compiler(value: &str) -> Result<Self, Error> {
        if value.is_empty() {
            return Err(Error::SavedFunctionAdmission(
                "TypeScript emitted no Saved Function declaration".to_string(),
            ));
        }
        if value.len() > SAVED_FUNCTION_DECLARATION_MAX_BYTES {
            return Err(Error::PackageTooLarge {
                component: "Saved Function declaration",
                maximum_bytes: SAVED_FUNCTION_DECLARATION_MAX_BYTES,
            });
        }
        Ok(Self(value.to_string()))
    }

    pub(crate) fn from_persisted(value: &str) -> Result<Self, Error> {
        Self::from_compiler(value).map_err(|error| {
            Error::CorruptData(format!("Saved Function has invalid declaration: {error}"))
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
pub struct SavedFunctionDeclarationField {
    path: coral_catalog::DeclarationPath,
    text: String,
}

impl SavedFunctionDeclarationField {
    fn from_compiler(field: &coral_v8::SavedFunctionDeclarationField) -> Result<Self, ()> {
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
pub enum SavedFunctionDeclarationFieldProjection {
    /// Searchable declaration fields were prepared successfully.
    Ready(Vec<SavedFunctionDeclarationField>),
    /// A stable private failure reason was retained without rejecting authoring.
    Failed(String),
}

impl SavedFunctionDeclarationFieldProjection {
    pub(crate) fn from_compiler(
        projection: Result<&[coral_v8::SavedFunctionDeclarationField], &str>,
    ) -> Self {
        let Ok(fields) = projection else {
            return Self::Failed("declaration_field_projection_failed".to_string());
        };
        let Ok(mut fields) = fields
            .iter()
            .map(SavedFunctionDeclarationField::from_compiler)
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

    pub(crate) fn fields(&self) -> Option<&[SavedFunctionDeclarationField]> {
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
        SAVED_FUNCTION_DECLARATION_FIELDS_FORMAT
    }

    pub(crate) fn from_storage(
        format: &str,
        status: &str,
        fields_json: Option<&str>,
        failure_reason: Option<&str>,
    ) -> Result<Self, Error> {
        if format != SAVED_FUNCTION_DECLARATION_FIELDS_FORMAT {
            return Err(Error::CorruptData(
                "Saved Function declaration fields have an incompatible format".to_string(),
            ));
        }
        match (status, fields_json, failure_reason) {
            ("ready", Some(json), None) => {
                let fields: Vec<SavedFunctionDeclarationField> = serde_json::from_str(json)
                    .map_err(|_error| {
                        Error::CorruptData(
                            "Saved Function declaration fields are not valid JSON".to_string(),
                        )
                    })?;
                if !valid_declaration_fields(&fields) {
                    return Err(Error::CorruptData(
                        "Saved Function declaration fields exceed their bounds".to_string(),
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
                "Saved Function has an invalid declaration-field projection outcome".to_string(),
            )),
        }
    }
}

fn valid_declaration_fields(fields: &[SavedFunctionDeclarationField]) -> bool {
    if fields.len() > SAVED_FUNCTION_DECLARATION_FIELD_MAX_COUNT {
        return false;
    }
    let mut previous = None;
    let mut total = 2usize;
    for field in fields {
        if field.text.is_empty()
            || field.text.len() > SAVED_FUNCTION_DECLARATION_FIELD_TEXT_MAX_BYTES
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
    total <= SAVED_FUNCTION_DECLARATION_FIELDS_MAX_BYTES
}

/// Stable category of one advisory TypeScript authoring diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SavedFunctionDiagnosticCategory {
    /// Informational compiler message.
    Message,
    /// Non-blocking warning.
    Warning,
    /// Ordinary strict semantic error retained without rejecting the save.
    Error,
    /// Non-blocking authoring suggestion.
    Suggestion,
}

/// One bounded advisory diagnostic retained with an accepted Saved Function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedFunctionDiagnostic {
    category: SavedFunctionDiagnosticCategory,
    code: u64,
    message: String,
    line: Option<u64>,
    column: Option<u64>,
}

impl SavedFunctionDiagnostic {
    pub(crate) fn from_compiler(diagnostic: &coral_v8::CompilerDiagnostic) -> Self {
        let category = match diagnostic.category() {
            coral_v8::DiagnosticCategory::Message => SavedFunctionDiagnosticCategory::Message,
            coral_v8::DiagnosticCategory::Warning => SavedFunctionDiagnosticCategory::Warning,
            coral_v8::DiagnosticCategory::Error => SavedFunctionDiagnosticCategory::Error,
            coral_v8::DiagnosticCategory::Suggestion => SavedFunctionDiagnosticCategory::Suggestion,
        };
        Self {
            category,
            code: diagnostic.code(),
            message: truncate_utf8(
                diagnostic.message(),
                SAVED_FUNCTION_DIAGNOSTIC_MESSAGE_MAX_BYTES,
            ),
            line: diagnostic.line(),
            column: diagnostic.column(),
        }
    }

    /// Returns the diagnostic category.
    #[must_use]
    pub const fn category(&self) -> SavedFunctionDiagnosticCategory {
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

/// Complete authoring input for one atomic create-or-replace operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedFunctionSave {
    path: SavedFunctionPath,
    description: SavedFunctionDescription,
    source: SavedFunctionSource,
}

impl SavedFunctionSave {
    /// Builds already-validated authoring input.
    #[must_use]
    pub const fn new(
        path: SavedFunctionPath,
        description: SavedFunctionDescription,
        source: SavedFunctionSource,
    ) -> Self {
        Self {
            path,
            description,
            source,
        }
    }

    pub(crate) const fn path(&self) -> &SavedFunctionPath {
        &self.path
    }

    pub(crate) const fn description(&self) -> &SavedFunctionDescription {
        &self.description
    }

    pub(crate) const fn source(&self) -> &SavedFunctionSource {
        &self.source
    }
}

/// Authenticated, correlated occurrence evidence for one authoring change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedFunctionChangeContext {
    actor: AuthenticatedPrincipal,
    occurred_at_unix_nanos: i64,
    trace_id: TraceId,
}

impl SavedFunctionChangeContext {
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

/// One Principal's current accepted Saved Function definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedFunction {
    id: SavedFunctionId,
    workspace_id: WorkspaceId,
    principal: Principal,
    path: SavedFunctionPath,
    description: SavedFunctionDescription,
    source: SavedFunctionSource,
    declaration: SavedFunctionDeclaration,
    declaration_field_projection: SavedFunctionDeclarationFieldProjection,
    diagnostics: Vec<SavedFunctionDiagnostic>,
    diagnostics_truncated: bool,
    saved_at_unix_nanos: i64,
    pub(crate) revision: u64,
}

impl SavedFunction {
    #[expect(
        clippy::too_many_arguments,
        reason = "the accepted current definition is assembled once at its persistence boundary"
    )]
    pub(crate) const fn new(
        id: SavedFunctionId,
        workspace_id: WorkspaceId,
        principal: Principal,
        path: SavedFunctionPath,
        description: SavedFunctionDescription,
        source: SavedFunctionSource,
        declaration: SavedFunctionDeclaration,
        declaration_field_projection: SavedFunctionDeclarationFieldProjection,
        diagnostics: Vec<SavedFunctionDiagnostic>,
        diagnostics_truncated: bool,
        saved_at_unix_nanos: i64,
        revision: u64,
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
        }
    }

    /// Returns the stable identity retained across replacements at this path.
    #[must_use]
    pub const fn id(&self) -> &SavedFunctionId {
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
    pub const fn path(&self) -> &SavedFunctionPath {
        &self.path
    }

    /// Returns the exact explicit description.
    #[must_use]
    pub const fn description(&self) -> &SavedFunctionDescription {
        &self.description
    }

    /// Returns the exact authored source.
    #[must_use]
    pub const fn source(&self) -> &SavedFunctionSource {
        &self.source
    }

    /// Returns the exact self-contained declaration.
    #[must_use]
    pub const fn declaration(&self) -> &SavedFunctionDeclaration {
        &self.declaration
    }

    /// Returns the non-blocking declaration-field projection outcome.
    #[must_use]
    pub const fn declaration_field_projection(&self) -> &SavedFunctionDeclarationFieldProjection {
        &self.declaration_field_projection
    }

    /// Returns bounded strict semantic diagnostics.
    #[must_use]
    pub fn diagnostics(&self) -> &[SavedFunctionDiagnostic] {
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
}

/// What Universal Search indexes about one current Saved Function. Never the source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedFunctionEntry {
    id: SavedFunctionId,
    workspace_id: WorkspaceId,
    principal: Principal,
    path: SavedFunctionPath,
    description: SavedFunctionDescription,
    declaration: SavedFunctionDeclaration,
    declaration_fields: Option<Vec<SavedFunctionDeclarationField>>,
    revision: u64,
}

impl SavedFunctionEntry {
    /// Returns what an accepted save hands to search after its commit.
    #[must_use]
    pub fn from_saved(function: &SavedFunction) -> Self {
        Self {
            id: function.id.clone(),
            workspace_id: function.workspace_id.clone(),
            principal: function.principal.clone(),
            path: function.path.clone(),
            description: function.description.clone(),
            declaration: function.declaration.clone(),
            declaration_fields: function
                .declaration_field_projection
                .fields()
                .map(<[_]>::to_vec),
            revision: function.revision,
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one entry is decoded once at its persistence boundary"
    )]
    pub(crate) const fn new(
        id: SavedFunctionId,
        workspace_id: WorkspaceId,
        principal: Principal,
        path: SavedFunctionPath,
        description: SavedFunctionDescription,
        declaration: SavedFunctionDeclaration,
        declaration_fields: Option<Vec<SavedFunctionDeclarationField>>,
        revision: u64,
    ) -> Self {
        Self {
            id,
            workspace_id,
            principal,
            path,
            description,
            declaration,
            declaration_fields,
            revision,
        }
    }

    /// Returns the stable identity retained across replacements at this path.
    #[must_use]
    pub const fn id(&self) -> &SavedFunctionId {
        &self.id
    }

    /// Returns the Workspace that owns the function.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    /// Returns the owning Principal, the only one who can see the function.
    #[must_use]
    pub const fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Returns the path below `coral.functions`, the rest of the Call Path.
    #[must_use]
    pub const fn path(&self) -> &SavedFunctionPath {
        &self.path
    }

    /// Returns the exact explicit description.
    #[must_use]
    pub const fn description(&self) -> &SavedFunctionDescription {
        &self.description
    }

    /// Returns the exact self-contained compiler-emitted declaration.
    #[must_use]
    pub const fn declaration(&self) -> &SavedFunctionDeclaration {
        &self.declaration
    }

    /// Returns the readable declaration paths with their text, or `None` when
    /// the save projected none or the stored projection is unreadable. The
    /// function is still searchable by path and description.
    #[must_use]
    pub fn declaration_fields(&self) -> Option<&[SavedFunctionDeclarationField]> {
        self.declaration_fields.as_deref()
    }

    /// Returns the definition revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// Current Principal-owned material supplied to ordinary Universal Search.
///
/// This projection contains only secret-free authoring contract fields. It
/// carries no source, diagnostics, Saved Function identity, revision,
/// Membership, Run, or accepted executable evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedFunctionAuthoringMaterial {
    path: SavedFunctionPath,
    description: SavedFunctionDescription,
    declaration: SavedFunctionDeclaration,
}

impl SavedFunctionAuthoringMaterial {
    pub(crate) const fn new(
        path: SavedFunctionPath,
        description: SavedFunctionDescription,
        declaration: SavedFunctionDeclaration,
    ) -> Self {
        Self {
            path,
            description,
            declaration,
        }
    }

    /// Returns the path relative to `coral.functions`.
    #[must_use]
    pub const fn path(&self) -> &SavedFunctionPath {
        &self.path
    }

    /// Returns the exact explicit authoring description.
    #[must_use]
    pub const fn description(&self) -> &SavedFunctionDescription {
        &self.description
    }

    /// Returns the exact self-contained compiler-emitted declaration.
    #[must_use]
    pub const fn declaration(&self) -> &SavedFunctionDeclaration {
        &self.declaration
    }
}

/// Most summaries one list page returns.
pub const SAVED_FUNCTION_PAGE_MAX_SIZE: u32 = 100;

/// Source-free summary of one current Saved Function for browsing.
///
/// It contains no source, declaration, or diagnostics; exact reads load those.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedFunctionSummary {
    path: SavedFunctionPath,
    description: SavedFunctionDescription,
    saved_at_unix_nanos: i64,
}

impl SavedFunctionSummary {
    pub(crate) const fn new(
        path: SavedFunctionPath,
        description: SavedFunctionDescription,
        saved_at_unix_nanos: i64,
    ) -> Self {
        Self {
            path,
            description,
            saved_at_unix_nanos,
        }
    }

    /// Returns the path relative to `coral.functions`.
    #[must_use]
    pub const fn path(&self) -> &SavedFunctionPath {
        &self.path
    }

    /// Returns the exact explicit description.
    #[must_use]
    pub const fn description(&self) -> &SavedFunctionDescription {
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
pub struct SavedFunctionPage {
    summaries: Vec<SavedFunctionSummary>,
    next_after: Option<SavedFunctionPath>,
}

impl SavedFunctionPage {
    pub(crate) const fn new(
        summaries: Vec<SavedFunctionSummary>,
        next_after: Option<SavedFunctionPath>,
    ) -> Self {
        Self {
            summaries,
            next_after,
        }
    }

    /// Returns this page's summaries in path order.
    #[must_use]
    pub fn summaries(&self) -> &[SavedFunctionSummary] {
        &self.summaries
    }

    /// Returns the path to pass as `after` for the next page, when one exists.
    #[must_use]
    pub const fn next_after(&self) -> Option<&SavedFunctionPath> {
        self.next_after.as_ref()
    }
}

/// Metadata-only Saved Function target supplied before listing or ranking.
///
/// This projection contains no source, declaration, relevance material,
/// diagnostics, revision, or accepted executable material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedFunctionSearchMaterial {
    id: SavedFunctionId,
    path: SavedFunctionPath,
    description: SavedFunctionDescription,
}

impl SavedFunctionSearchMaterial {
    pub(crate) const fn new(
        id: SavedFunctionId,
        path: SavedFunctionPath,
        description: SavedFunctionDescription,
    ) -> Self {
        Self {
            id,
            path,
            description,
        }
    }

    /// Returns the Saved Function's stable identity.
    #[must_use]
    pub const fn id(&self) -> &SavedFunctionId {
        &self.id
    }

    /// Returns the path relative to `coral.functions`.
    #[must_use]
    pub const fn path(&self) -> &SavedFunctionPath {
        &self.path
    }

    /// Returns the exact explicit description.
    #[must_use]
    pub const fn description(&self) -> &SavedFunctionDescription {
        &self.description
    }
}

/// Per-function declaration-lane material loaded only for relevance search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedFunctionDeclarationSearchMaterial {
    path: SavedFunctionPath,
    declaration_fields: Option<Vec<SavedFunctionDeclarationField>>,
}

impl SavedFunctionDeclarationSearchMaterial {
    pub(crate) const fn new(
        path: SavedFunctionPath,
        declaration_fields: Option<Vec<SavedFunctionDeclarationField>>,
    ) -> Self {
        Self {
            path,
            declaration_fields,
        }
    }

    /// Returns the path relative to `coral.functions`.
    #[must_use]
    pub const fn path(&self) -> &SavedFunctionPath {
        &self.path
    }

    /// Returns fields when this function's declaration lane is healthy.
    #[must_use]
    pub fn declaration_fields(&self) -> Option<&[SavedFunctionDeclarationField]> {
        self.declaration_fields.as_deref()
    }
}

/// Exact owner-editing material supplied only to exact description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactSavedFunctionAuthoringMaterial {
    source: SavedFunctionSource,
    diagnostics: Vec<SavedFunctionDiagnostic>,
    diagnostics_truncated: bool,
}

impl ExactSavedFunctionAuthoringMaterial {
    pub(crate) const fn new(
        source: SavedFunctionSource,
        diagnostics: Vec<SavedFunctionDiagnostic>,
        diagnostics_truncated: bool,
    ) -> Self {
        Self {
            source,
            diagnostics,
            diagnostics_truncated,
        }
    }

    /// Returns the exact current source for authorized exact description.
    #[must_use]
    pub const fn source(&self) -> &SavedFunctionSource {
        &self.source
    }

    /// Returns bounded advisory authoring diagnostics.
    #[must_use]
    pub fn diagnostics(&self) -> &[SavedFunctionDiagnostic] {
        &self.diagnostics
    }

    /// Returns whether additional diagnostics were omitted by the authoring bound.
    #[must_use]
    pub const fn diagnostics_truncated(&self) -> bool {
        self.diagnostics_truncated
    }
}

/// Complete relevance-independent exact Saved Function description material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactSavedFunctionDescriptionMaterial {
    authoring: SavedFunctionAuthoringMaterial,
    detail: ExactSavedFunctionAuthoringMaterial,
}

impl ExactSavedFunctionDescriptionMaterial {
    pub(crate) const fn new(
        authoring: SavedFunctionAuthoringMaterial,
        detail: ExactSavedFunctionAuthoringMaterial,
    ) -> Self {
        Self { authoring, detail }
    }

    /// Returns source-free public authoring material.
    #[must_use]
    pub const fn authoring(&self) -> &SavedFunctionAuthoringMaterial {
        &self.authoring
    }

    /// Returns source and diagnostics for exact owner editing.
    #[must_use]
    pub const fn detail(&self) -> &ExactSavedFunctionAuthoringMaterial {
        &self.detail
    }
}

/// Accepted Saved Function lifecycle action.
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
            Self::Created => "saved_function.created",
            Self::Replaced => "saved_function.replaced",
            Self::Deleted => "saved_function.deleted",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "saved_function.created" => Ok(Self::Created),
            "saved_function.replaced" => Ok(Self::Replaced),
            "saved_function.deleted" => Ok(Self::Deleted),
            _ => Err(Error::CorruptData(format!(
                "Saved Function lifecycle Event has unrecognized action '{value}'"
            ))),
        }
    }
}

/// Accepted result carried by a Saved Function lifecycle Event.
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
                "Saved Function lifecycle Event has unrecognized outcome '{value}'"
            ))),
        }
    }
}

/// Safe immutable evidence of one accepted Saved Function authoring change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdministrativeLifecycleEvent {
    workspace_id: WorkspaceId,
    actor: Principal,
    action: LifecycleAction,
    outcome: LifecycleOutcome,
    target_id: SavedFunctionId,
    target_path: SavedFunctionPath,
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
        target_id: SavedFunctionId,
        target_path: SavedFunctionPath,
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

    /// Returns the stable Saved Function target identity.
    #[must_use]
    pub const fn target_id(&self) -> &SavedFunctionId {
        &self.target_id
    }

    /// Returns the safe target path without source or description.
    #[must_use]
    pub const fn target_path(&self) -> &SavedFunctionPath {
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

/// Current source selected for compilation in an authorized read snapshot.
///
/// Its private revision is only acceptance evidence, not a public version or edit
/// token. Run retains the identity and source in its own durable accepted package.
#[derive(Debug, Clone)]
pub struct SavedFunctionSelection {
    pub(crate) id: SavedFunctionId,
    pub(crate) path: SavedFunctionPath,
    pub(crate) source: SavedFunctionSource,
    pub(crate) revision: u64,
}

impl SavedFunctionSelection {
    /// Returns the selected stable identity for retained Run attribution.
    #[must_use]
    pub const fn id(&self) -> &SavedFunctionId {
        &self.id
    }

    /// Returns the selected path for static linking.
    #[must_use]
    pub const fn path(&self) -> &SavedFunctionPath {
        &self.path
    }

    /// Returns the exact selected authored source.
    #[must_use]
    pub const fn source(&self) -> &SavedFunctionSource {
        &self.source
    }
}

impl SavedFunctionSelection {
    pub(crate) fn from_current(function: &SavedFunction) -> Self {
        Self {
            id: function.id.clone(),
            path: function.path.clone(),
            source: function.source.clone(),
            revision: function.revision,
        }
    }
}
