use std::collections::HashSet;
use std::fmt;

use base64::Engine as _;
use coral_catalog::{ToolHints, ToolPresentation};
pub use coral_provider::ProviderId;
pub use coral_provider::access::ProviderCallRoot;
use coral_provider::access::ProviderNamespace;

pub use coral_functions::ExactSavedFunctionAuthoringMaterial as ExactSavedFunctionDetail;

use crate::Error;

#[cfg(test)]
mod tests;

/// Maximum UTF-8 byte length accepted by Universal Search.
pub const SEARCH_QUERY_MAX_BYTES: usize = 4 * 1024;
/// Default maximum number of Search Hits in one Universal Search page.
pub const SEARCH_DEFAULT_LIMIT: u16 = 10;
/// Largest caller-selected result count for one Universal Search page.
pub const SEARCH_RESULT_LIMIT_MAX: u16 = 50;
/// Conservative transport-neutral response budget for one search page.
pub const SEARCH_PAGE_MAX_BYTES: usize = 2 * 1024 * 1024;
/// Maximum accepted opaque cursor size.
pub const SEARCH_CURSOR_MAX_BYTES: usize = 8 * 1024;
/// Maximum metadata field names retained as evidence for one match.
pub const SEARCH_EVIDENCE_FIELDS_MAX: usize = 16;
/// Maximum declaration paths retained as evidence for one match.
pub const SEARCH_EVIDENCE_PATHS_MAX: usize = 32;
/// Maximum UTF-8 bytes retained in one evidence path.
pub const SEARCH_EVIDENCE_PATH_MAX_BYTES: usize = coral_catalog::DECLARATION_PATH_MAX_BYTES;

/// One optional bounded Universal Search query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchQuery(String);

impl SearchQuery {
    /// Validates one nonblank, null-free, bounded query.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] when the query is blank, contains a null
    /// character, or exceeds the fixed bound.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.trim().is_empty() || value.contains('\0') || value.len() > SEARCH_QUERY_MAX_BYTES {
            return Err(Error::InvalidInput(format!(
                "search query must be non-blank, contain no null character, and be at most {SEARCH_QUERY_MAX_BYTES} UTF-8 bytes"
            )));
        }
        Ok(Self(value.to_string()))
    }

    /// Returns the exact accepted query.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Positive bounded maximum result count for one search page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchLimit(u16);

impl SearchLimit {
    /// Validates one caller-selected result count.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for zero or a value above the fixed maximum.
    pub fn parse(value: u16) -> Result<Self, Error> {
        if value == 0 || value > SEARCH_RESULT_LIMIT_MAX {
            return Err(Error::InvalidInput(format!(
                "search limit must be between 1 and {SEARCH_RESULT_LIMIT_MAX}"
            )));
        }
        Ok(Self(value))
    }

    /// Returns the accepted caller-selected page limit.
    #[must_use]
    pub const fn value(self) -> u16 {
        self.0
    }

    pub(crate) const fn get(self) -> usize {
        self.0 as usize
    }
}

impl Default for SearchLimit {
    fn default() -> Self {
        Self(SEARCH_DEFAULT_LIMIT)
    }
}

/// Opaque query-bound Universal Search continuation state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchCursor(String);

impl SearchCursor {
    /// Strictly validates one canonical unpadded base64url cursor.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidCursor`] for empty, oversized, noncanonical, or
    /// malformed data.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.is_empty()
            || value.len() > SEARCH_CURSOR_MAX_BYTES
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(Error::InvalidCursor);
        }
        let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_error| Error::InvalidCursor)?;
        if base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(decoded) != value {
            return Err(Error::InvalidCursor);
        }
        Ok(Self(value.to_string()))
    }

    pub(crate) fn encoded(value: &str) -> Result<Self, Error> {
        Self::parse(value)
    }

    /// Returns the opaque transport value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SearchCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// One bounded Universal Search request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchRequest {
    query: Option<SearchQuery>,
    provider: Option<ProviderCallRoot>,
    limit: SearchLimit,
    cursor: Option<SearchCursor>,
}

/// Unvalidated operation input. The owner validates this after attributed admission begins.
/// This is not a transport envelope and contains no authentication or Task fields.
#[derive(Debug, Clone, Default)]
pub struct SearchInput {
    /// Optional caller query, preserved exactly when accepted.
    pub query: Option<String>,
    /// Optional Provider filter: a namespace, such as `github`, or `coral.workspace`.
    pub provider: Option<String>,
    /// Optional requested page size.
    pub limit: Option<u16>,
    /// Optional continuation from an earlier page.
    pub cursor: Option<String>,
}

impl TryFrom<SearchInput> for SearchRequest {
    type Error = Error;
    fn try_from(input: SearchInput) -> Result<Self, Error> {
        let mut request = match input.query {
            Some(query) => Self::query(SearchQuery::parse(&query)?),
            None => Self::list(),
        };
        if let Some(provider) = input.provider {
            request = request.with_provider(provider_filter(&provider)?);
        }
        if let Some(limit) = input.limit {
            request = request.with_limit(SearchLimit::parse(limit)?);
        }
        if let Some(cursor) = input.cursor {
            request = request.with_cursor(SearchCursor::parse(&cursor)?);
        }
        Ok(request)
    }
}

/// Parses the `provider` filter: a Provider Namespace, such as `github`, or
/// `coral.workspace` for the Built-in Provider. A complete `coral.providers` Call
/// Root is rejected with the namespace to use instead.
fn provider_filter(value: &str) -> Result<ProviderCallRoot, Error> {
    match ProviderCallRoot::parse(value) {
        Ok(ProviderCallRoot::Builtin) => Ok(ProviderCallRoot::Builtin),
        Ok(ProviderCallRoot::Owner(namespace)) => Err(Error::InvalidInput(format!(
            "provider must be a Provider namespace, such as `{}`",
            namespace.as_str()
        ))),
        Err(_not_a_call_root) => ProviderNamespace::parse(value)
            .map(ProviderCallRoot::Owner)
            .map_err(|_invalid_namespace| Error::InvalidInput("invalid Provider namespace".into())),
    }
}

/// The `provider` filter value that selects the Provider at this Call Root:
/// its namespace, such as `github`, or `coral.workspace` for the Built-in Provider.
/// Callers see this value; the Call Root stays internal.
#[must_use]
pub fn provider_filter_value(call_root: &ProviderCallRoot) -> &str {
    call_root
        .namespace()
        .map_or(call_root.as_str(), ProviderNamespace::as_str)
}

impl SearchRequest {
    /// Builds a listing request with no relevance query.
    #[must_use]
    pub fn list() -> Self {
        Self::default()
    }

    /// Builds a request for one validated query.
    #[must_use]
    pub fn query(query: SearchQuery) -> Self {
        Self {
            query: Some(query),
            ..Self::default()
        }
    }

    /// Restricts results to the Tools of the Provider with this Call Root.
    #[must_use]
    pub fn with_provider(mut self, provider: ProviderCallRoot) -> Self {
        self.provider = Some(provider);
        self
    }

    /// Applies one validated result-count bound.
    #[must_use]
    pub const fn with_limit(mut self, limit: SearchLimit) -> Self {
        self.limit = limit;
        self
    }

    /// Continues one exactly matching prior request.
    #[must_use]
    pub fn with_cursor(mut self, cursor: SearchCursor) -> Self {
        self.cursor = Some(cursor);
        self
    }

    /// Returns the optional exact query.
    #[must_use]
    pub const fn search_query(&self) -> Option<&SearchQuery> {
        self.query.as_ref()
    }

    /// Returns the optional Provider Call Root filter.
    #[must_use]
    pub const fn provider(&self) -> Option<&ProviderCallRoot> {
        self.provider.as_ref()
    }

    /// Returns the page's hit count limit.
    #[must_use]
    pub const fn limit(&self) -> SearchLimit {
        self.limit
    }

    /// Returns optional opaque continuation state.
    #[must_use]
    pub const fn cursor(&self) -> Option<&SearchCursor> {
        self.cursor.as_ref()
    }
}

/// Complete public JavaScript path for one Tool or Saved Function.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CallPath(String);

impl CallPath {
    /// Parses one complete Call Path: a Provider Call Root or `coral.functions`
    /// followed by the relative path.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] unless every path member is a safe
    /// JavaScript identifier under `coral` with a root and a relative path.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let segments = value.split('.').collect::<Vec<_>>();
        let shaped = ProviderCallRoot::split_call_path(value).is_some()
            || matches!(segments.as_slice(), ["coral", "functions", _, ..]);
        if !shaped || segments.iter().any(|segment| !safe_path_segment(segment)) {
            return Err(Error::InvalidInput(
                "Call Path must be a complete safe coral.providers, coral.workspace or coral.functions JavaScript path"
                    .to_string(),
            ));
        }
        Ok(Self(value.to_string()))
    }

    pub(crate) fn tool(call_root: &ProviderCallRoot, relative_path: &str) -> Result<Self, Error> {
        Self::parse(&format!("{}.{relative_path}", call_root.as_str()))
    }

    pub(crate) fn saved_function(relative_path: &str) -> Result<Self, Error> {
        Self::parse(&format!("coral.functions.{relative_path}"))
    }

    /// Returns the last segment: the name a signature line calls it by.
    pub(crate) fn leaf(&self) -> &str {
        self.0.rsplit('.').next().unwrap_or(&self.0)
    }

    /// Returns the complete canonical JavaScript path.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
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
}

/// Kind of Search Entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntityKind {
    /// One Provider Tool projected through a Provider.
    ProviderTool,
    /// One current Principal-owned Saved Function.
    SavedFunction,
}

/// Current approval fact exposed for one visible Tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalRequirement {
    /// Current policy permits admission without an Invocation Approval.
    NotRequired,
    /// Current policy requires an exact Invocation Approval.
    Required,
}

/// Complete safe TypeScript authoring contract for one visible Tool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolAuthoringDescription {
    call_path: CallPath,
    typescript_declaration: String,
    provider_call_root: ProviderCallRoot,
    provider_id: ProviderId,
    provider_display_name: String,
    presentation: ToolPresentation,
    hints: ToolHints,
    approval_requirement: ApprovalRequirement,
}

impl ToolAuthoringDescription {
    #[expect(
        clippy::too_many_arguments,
        reason = "one immutable complete Tool Authoring Description is assembled at discovery"
    )]
    pub(crate) const fn new(
        call_path: CallPath,
        typescript_declaration: String,
        provider_call_root: ProviderCallRoot,
        provider_id: ProviderId,
        provider_display_name: String,
        presentation: ToolPresentation,
        hints: ToolHints,
        approval_requirement: ApprovalRequirement,
    ) -> Self {
        Self {
            call_path,
            typescript_declaration,
            provider_call_root,
            provider_id,
            provider_display_name,
            presentation,
            hints,
            approval_requirement,
        }
    }

    /// Returns the complete callable path.
    #[must_use]
    pub const fn call_path(&self) -> &CallPath {
        &self.call_path
    }

    /// Returns the optional Provider-authored title.
    #[must_use]
    pub fn title(&self) -> Option<&str> {
        self.presentation.title()
    }

    /// Returns the safe complete Provider-authored description.
    #[must_use]
    pub fn description(&self) -> &str {
        self.presentation.description()
    }

    /// Returns the TypeScript declaration: complete in a Search Hit, and
    /// abridged in an exact description.
    #[must_use]
    pub fn typescript_declaration(&self) -> &str {
        &self.typescript_declaration
    }

    /// Returns the immutable Provider Call Root.
    #[must_use]
    pub const fn provider_call_root(&self) -> &ProviderCallRoot {
        &self.provider_call_root
    }

    /// Returns the underlying Provider identity without a Generation selector.
    #[must_use]
    pub const fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }

    /// Returns safe Provider presentation metadata.
    #[must_use]
    pub fn provider_display_name(&self) -> &str {
        &self.provider_display_name
    }

    /// Returns current Provider-authored deprecation evidence.
    #[must_use]
    pub const fn deprecated(&self) -> bool {
        self.presentation.deprecated()
    }

    /// Returns safe Provider-authored tags.
    #[must_use]
    pub fn tags(&self) -> &[String] {
        self.presentation.tags()
    }

    /// Returns optional safe Provider-authored usage guidance.
    #[must_use]
    pub fn guidance(&self) -> Option<&str> {
        self.presentation.guidance()
    }

    /// Returns the declared effect hints.
    #[must_use]
    pub const fn hints(&self) -> ToolHints {
        self.hints
    }

    /// Returns whether the current policy requires exact Invocation Approval.
    #[must_use]
    pub const fn approval_requirement(&self) -> ApprovalRequirement {
        self.approval_requirement
    }
}

/// Complete TypeScript authoring contract for one current Saved Function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedFunctionAuthoringDescription {
    call_path: CallPath,
    description: String,
    typescript_declaration: String,
}

impl SavedFunctionAuthoringDescription {
    pub(crate) const fn new(
        call_path: CallPath,
        description: String,
        typescript_declaration: String,
    ) -> Self {
        Self {
            call_path,
            description,
            typescript_declaration,
        }
    }

    /// Returns the complete callable path.
    #[must_use]
    pub const fn call_path(&self) -> &CallPath {
        &self.call_path
    }

    /// Returns the exact explicit Saved Function description.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns the compiler-emitted declaration, abridged in an exact
    /// description.
    #[must_use]
    pub fn typescript_declaration(&self) -> &str {
        &self.typescript_declaration
    }
}

/// One complete current TypeScript Authoring Description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthoringDescription {
    /// A visible Tool contract.
    Tool(ToolAuthoringDescription),
    /// The authenticated Principal's current Saved Function contract.
    SavedFunction(SavedFunctionAuthoringDescription),
}

impl AuthoringDescription {
    /// Returns the callable kind.
    #[must_use]
    pub const fn kind(&self) -> EntityKind {
        match self {
            Self::Tool(_) => EntityKind::ProviderTool,
            Self::SavedFunction(_) => EntityKind::SavedFunction,
        }
    }

    /// Returns the complete Call Path.
    #[must_use]
    pub const fn call_path(&self) -> &CallPath {
        match self {
            Self::Tool(description) => description.call_path(),
            Self::SavedFunction(description) => description.call_path(),
        }
    }

    /// Returns the safe complete authoring description.
    #[must_use]
    pub fn description(&self) -> &str {
        match self {
            Self::Tool(description) => description.description(),
            Self::SavedFunction(description) => description.description(),
        }
    }

    /// Returns the TypeScript declaration.
    #[must_use]
    pub fn typescript_declaration(&self) -> &str {
        match self {
            Self::Tool(description) => description.typescript_declaration(),
            Self::SavedFunction(description) => description.typescript_declaration(),
        }
    }
}

/// Query-specific evidence for one Universal Search match.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchEvidence {
    fields: Vec<String>,
    paths: Vec<String>,
}

impl SearchEvidence {
    pub(crate) fn new(fields: Vec<String>, paths: Vec<String>) -> Self {
        Self {
            fields: bounded_unique(fields, SEARCH_EVIDENCE_FIELDS_MAX, None),
            paths: bounded_unique(
                paths,
                SEARCH_EVIDENCE_PATHS_MAX,
                Some(SEARCH_EVIDENCE_PATH_MAX_BYTES),
            ),
        }
    }

    /// Returns matched public metadata field names.
    #[must_use]
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// Returns matched readable declaration paths.
    #[must_use]
    pub fn paths(&self) -> &[String] {
        &self.paths
    }
}

fn bounded_unique(
    values: Vec<String>,
    maximum: usize,
    maximum_bytes: Option<usize>,
) -> Vec<String> {
    let mut seen = HashSet::new();
    values
        .into_iter()
        .filter(|value| maximum_bytes.is_none_or(|bound| value.len() <= bound))
        .filter(|value| seen.insert(value.clone()))
        .take(maximum)
        .collect()
}

/// One item of a search page: enough to call a read from its signature line
/// when that line is complete, or to know that describe comes first. Never
/// the declaration or the types it references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchHit {
    call_path: CallPath,
    signature_line: String,
    description_snippet: String,
    guidance_snippet: Option<String>,
    matched_tags: Vec<String>,
    evidence: SearchEvidence,
    tool: Option<ToolHit>,
}

/// What a Search Hit shows only for a Tool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolHit {
    provider_display_name: String,
    title: Option<String>,
    hints: ToolHints,
    approval_requirement: ApprovalRequirement,
    deprecated: bool,
}

impl ToolHit {
    /// Returns safe Provider presentation metadata.
    #[must_use]
    pub fn provider_display_name(&self) -> &str {
        &self.provider_display_name
    }

    /// Returns the optional Provider-authored title, always whole.
    #[must_use]
    pub fn title(&self) -> Option<&str> {
        self.title.as_deref()
    }

    /// Returns the declared effect hints.
    #[must_use]
    pub const fn hints(&self) -> ToolHints {
        self.hints
    }

    /// Returns whether the current policy requires exact Invocation Approval.
    #[must_use]
    pub const fn approval_requirement(&self) -> ApprovalRequirement {
        self.approval_requirement
    }

    /// Returns current Provider-authored deprecation evidence.
    #[must_use]
    pub const fn deprecated(&self) -> bool {
        self.deprecated
    }
}

/// The text a Search Hit shows, derived at hydration.
pub(crate) struct HitText {
    pub(crate) signature_line: String,
    pub(crate) description_snippet: String,
    pub(crate) guidance_snippet: Option<String>,
    pub(crate) matched_tags: Vec<String>,
}

impl SearchHit {
    pub(crate) fn from_tool(
        description: &ToolAuthoringDescription,
        text: HitText,
        evidence: SearchEvidence,
    ) -> Self {
        Self::new(
            description.call_path.clone(),
            text,
            evidence,
            Some(ToolHit {
                provider_display_name: description.provider_display_name.clone(),
                title: description.title().map(str::to_string),
                hints: description.hints,
                approval_requirement: description.approval_requirement,
                deprecated: description.deprecated(),
            }),
        )
    }

    pub(crate) fn from_saved_function(
        call_path: CallPath,
        text: HitText,
        evidence: SearchEvidence,
    ) -> Self {
        Self::new(call_path, text, evidence, None)
    }

    fn new(
        call_path: CallPath,
        text: HitText,
        evidence: SearchEvidence,
        tool: Option<ToolHit>,
    ) -> Self {
        Self {
            call_path,
            signature_line: text.signature_line,
            description_snippet: text.description_snippet,
            guidance_snippet: text.guidance_snippet,
            matched_tags: text.matched_tags,
            evidence,
            tool,
        }
    }

    /// Returns the Search Entity kind.
    #[must_use]
    pub const fn kind(&self) -> EntityKind {
        if self.tool.is_some() {
            EntityKind::ProviderTool
        } else {
            EntityKind::SavedFunction
        }
    }

    /// Returns the complete callable path.
    #[must_use]
    pub const fn call_path(&self) -> &CallPath {
        &self.call_path
    }

    /// Returns the bounded signature line. It is complete when it has no cut
    /// marker and names no type it does not show.
    #[must_use]
    pub fn signature_line(&self) -> &str {
        &self.signature_line
    }

    /// Returns the bounded window of the description.
    #[must_use]
    pub fn description_snippet(&self) -> &str {
        &self.description_snippet
    }

    /// Returns the bounded window of the guidance, only when a query term
    /// matched in it.
    #[must_use]
    pub fn guidance_snippet(&self) -> Option<&str> {
        self.guidance_snippet.as_deref()
    }

    /// Returns the tags a query term matched.
    #[must_use]
    pub fn matched_tags(&self) -> &[String] {
        &self.matched_tags
    }

    /// Returns metadata fields and declaration paths that explain this match.
    #[must_use]
    pub const fn evidence(&self) -> &SearchEvidence {
        &self.evidence
    }

    /// Returns what the hit shows only for a Tool.
    #[must_use]
    pub const fn tool(&self) -> Option<&ToolHit> {
        self.tool.as_ref()
    }

    pub(crate) fn encoded_size_upper_bound(&self) -> usize {
        let tool = self.tool.as_ref();
        [
            self.call_path.as_str(),
            self.signature_line.as_str(),
            self.description_snippet.as_str(),
            self.guidance_snippet.as_deref().unwrap_or_default(),
            tool.map_or("", |tool| tool.provider_display_name.as_str()),
            tool.and_then(|tool| tool.title.as_deref())
                .unwrap_or_default(),
        ]
        .into_iter()
        .chain(self.matched_tags.iter().map(String::as_str))
        .chain(self.evidence.fields.iter().map(String::as_str))
        .chain(self.evidence.paths.iter().map(String::as_str))
        .fold(1_024usize, |total, value| {
            total.saturating_add(value.len().saturating_mul(6))
        })
    }
}

/// What one retriever reports about its part of a search page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetrieverState {
    /// The retriever ranked at least one Search Entity.
    ResultsFound,
    /// The retriever searched every unit and nothing matched.
    Empty,
    /// An earlier Coral's Provider filter left this kind out. No search
    /// reports it now; a search retained before then holds it.
    Skipped,
    /// Some units could not be searched: not yet indexed, or failed.
    Partial,
    /// The retriever could not rank at all.
    Error,
}

/// How much of its kind one retriever searched for a page.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetrieverCoverage {
    eligible_units: u64,
    searched_units: u64,
    failed_units: u64,
    returned_count: u64,
    has_more: bool,
}

impl RetrieverCoverage {
    pub(crate) const fn new(
        eligible_units: u64,
        searched_units: u64,
        returned_count: u64,
        has_more: bool,
    ) -> Self {
        Self {
            eligible_units,
            searched_units,
            failed_units: eligible_units.saturating_sub(searched_units),
            returned_count,
            has_more,
        }
    }

    /// Returns the units the caller may search: visible, ready Providers for
    /// Tools; the Principal's current Saved Functions for functions.
    #[must_use]
    pub const fn eligible_units(&self) -> u64 {
        self.eligible_units
    }

    /// Returns the eligible units the retriever searched.
    #[must_use]
    pub const fn searched_units(&self) -> u64 {
        self.searched_units
    }

    /// Returns the eligible units not yet indexed or failed.
    #[must_use]
    pub const fn failed_units(&self) -> u64 {
        self.failed_units
    }

    /// Returns how many Search Entities the retriever handed over.
    #[must_use]
    pub const fn returned_count(&self) -> u64 {
        self.returned_count
    }

    /// Returns whether the retriever's window cut its ranking short.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }
}

/// One retriever's state and coverage on a search page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetrieverStatus {
    retriever: EntityKind,
    state: RetrieverState,
    coverage: RetrieverCoverage,
}

impl RetrieverStatus {
    pub(crate) const fn new(
        retriever: EntityKind,
        state: RetrieverState,
        coverage: RetrieverCoverage,
    ) -> Self {
        Self {
            retriever,
            state,
            coverage,
        }
    }

    /// Returns the Search Entity kind the retriever ranks.
    #[must_use]
    pub const fn retriever(&self) -> EntityKind {
        self.retriever
    }

    /// Returns what the retriever reports.
    #[must_use]
    pub const fn state(&self) -> RetrieverState {
        self.state
    }

    /// Returns how much the retriever searched.
    #[must_use]
    pub const fn coverage(&self) -> &RetrieverCoverage {
        &self.coverage
    }
}

/// One bounded page of Search Hits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchPage {
    hits: Vec<SearchHit>,
    next_cursor: Option<SearchCursor>,
    has_more: bool,
    retriever_statuses: Vec<RetrieverStatus>,
}

impl SearchPage {
    pub(crate) const fn new(
        hits: Vec<SearchHit>,
        next_cursor: Option<SearchCursor>,
        has_more: bool,
        retriever_statuses: Vec<RetrieverStatus>,
    ) -> Self {
        Self {
            hits,
            next_cursor,
            has_more,
            retriever_statuses,
        }
    }

    /// Returns one status per retriever, in retriever order.
    #[must_use]
    pub fn retriever_statuses(&self) -> &[RetrieverStatus] {
        &self.retriever_statuses
    }

    /// Returns the hits that fit both page bounds, in rank order.
    #[must_use]
    pub fn hits(&self) -> &[SearchHit] {
        &self.hits
    }

    /// Returns opaque continuation state when current results remain.
    #[must_use]
    pub const fn next_cursor(&self) -> Option<&SearchCursor> {
        self.next_cursor.as_ref()
    }

    /// Returns whether the current result universe has another candidate.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }
}

/// Exact current result of describing one complete Call Path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactAuthoringDescription {
    authoring: AuthoringDescription,
    saved_function_detail: Option<ExactSavedFunctionDetail>,
}

impl ExactAuthoringDescription {
    pub(crate) const fn tool(authoring: ToolAuthoringDescription) -> Self {
        Self {
            authoring: AuthoringDescription::Tool(authoring),
            saved_function_detail: None,
        }
    }

    pub(crate) const fn saved_function(
        authoring: SavedFunctionAuthoringDescription,
        detail: ExactSavedFunctionDetail,
    ) -> Self {
        Self {
            authoring: AuthoringDescription::SavedFunction(authoring),
            saved_function_detail: Some(detail),
        }
    }

    /// The same description, with its declaration abridged, or cut to the
    /// `types` it names. A description cut to `types` leaves out a Saved
    /// Function's source and diagnostics, which the caller already has.
    pub(crate) fn abridge(mut self, types: &[String]) -> Result<Self, crate::Error> {
        if !types.is_empty() {
            self.saved_function_detail = None;
        }
        let kind = self.authoring.kind();
        let declaration = match &mut self.authoring {
            AuthoringDescription::Tool(description) => &mut description.typescript_declaration,
            AuthoringDescription::SavedFunction(description) => {
                &mut description.typescript_declaration
            }
        };
        *declaration = crate::signature::abridge(declaration, kind, types)?;
        Ok(self)
    }

    /// Returns the current callable contract.
    #[must_use]
    pub const fn authoring(&self) -> &AuthoringDescription {
        &self.authoring
    }

    /// Returns editable Saved Function detail for an exact owned Saved Function.
    #[must_use]
    pub const fn saved_function_detail(&self) -> Option<&ExactSavedFunctionDetail> {
        self.saved_function_detail.as_ref()
    }
}

/// Whether search has indexed a Provider's active Generation. Search prepares
/// a Generation's entries after the commit that activates it (ADR 0017), so
/// the state lags publication and never gates a call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchIndexState {
    /// No attempt has reached the Generation yet.
    Pending,
    /// The last attempt indexed the Generation's Tools.
    Indexed,
    /// The last attempt failed; the Tools are missing from search until
    /// the reconcile task retries.
    Failed {
        /// The kind of error the attempt recorded; `None` when the row holds
        /// no kind this Coral knows. Never the error's message, which can
        /// hold internal detail.
        reason: Option<SearchIndexFailure>,
    },
}

/// The kind of error that failed a Generation's indexing attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchIndexFailure {
    /// An entry could not become a row, for example an invalid Call Path.
    InvalidEntry,
    /// The write did not finish within its limit.
    TimedOut,
    /// The system clock could not stamp the attempt.
    Clock,
    /// A search-owned row held a value its column does not admit.
    Corrupt,
    /// Reading or preparing the Provider's Generation failed.
    Provider,
    /// Database access failed.
    Database,
}
