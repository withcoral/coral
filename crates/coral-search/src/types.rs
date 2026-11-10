use std::collections::HashSet;

use coral_catalog::{AuthoredToolKind, ToolHints, ToolPresentation};
pub use coral_provider::ProviderId;
pub use coral_provider::access::ProviderCallRoot;
use coral_provider::access::ProviderNamespace;

pub use coral_tools::ExactAuthoredToolAuthoringMaterial as ExactAuthoredToolDetail;

use crate::Error;
use crate::signature::SignatureLine;

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

/// The `provider` filter: namespaces as the caller wrote them, such as
/// `github`, or `coral.workspace` for the Built-in Provider. Each value is
/// compared with the namespaces the caller can search, so a value that names
/// none of them, such as a complete Call Root, is refused with that list.
/// An empty filter leaves every Provider.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderFilter(Vec<String>);

impl ProviderFilter {
    /// Bounds one caller-written filter.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for more than [`SEARCH_PROVIDERS_MAX`]
    /// values.
    pub fn new(values: Vec<String>) -> Result<Self, Error> {
        if values.len() > SEARCH_PROVIDERS_MAX {
            return Err(Error::InvalidInput(format!(
                "provider names at most {SEARCH_PROVIDERS_MAX} namespaces"
            )));
        }
        Ok(Self(values))
    }

    /// The filter that leaves only the Provider at this Call Root.
    #[must_use]
    pub fn one(call_root: &ProviderCallRoot) -> Self {
        Self(vec![provider_filter_value(call_root).to_owned()])
    }

    /// The values in the order the caller wrote them.
    #[must_use]
    pub fn values(&self) -> &[String] {
        &self.0
    }

    /// Whether the filter leaves the Provider at this Call Root.
    pub(crate) fn admits(&self, call_root: &ProviderCallRoot) -> bool {
        self.0.is_empty()
            || self
                .0
                .iter()
                .any(|value| value == provider_filter_value(call_root))
    }

    /// The positions of the values that name none of these Call Roots.
    pub(crate) fn unmatched(&self, call_roots: &[&ProviderCallRoot]) -> Vec<usize> {
        self.0
            .iter()
            .enumerate()
            .filter(|(_position, value)| {
                !call_roots
                    .iter()
                    .any(|call_root| *value == provider_filter_value(call_root))
            })
            .map(|(position, _value)| position)
            .collect()
    }
}

/// One Provider the caller can search: the `provider` filter value that
/// names it and its display name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchableProvider {
    call_root: ProviderCallRoot,
    display_name: String,
}

impl SearchableProvider {
    /// One Provider the caller can search, by its Call Root and display
    /// name.
    #[must_use]
    pub const fn new(call_root: ProviderCallRoot, display_name: String) -> Self {
        Self {
            call_root,
            display_name,
        }
    }

    /// The Provider's Call Root.
    #[must_use]
    pub const fn call_root(&self) -> &ProviderCallRoot {
        &self.call_root
    }

    /// The value a `provider` filter names it by: its namespace, or
    /// `coral.workspace` for the Built-in Provider.
    #[must_use]
    pub fn namespace(&self) -> &str {
        provider_filter_value(&self.call_root)
    }

    /// The display name a person gave the Provider. It is text a Workspace
    /// member wrote.
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }
}

/// One bounded Universal Search request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchRequest {
    query: Option<SearchQuery>,
    provider: ProviderFilter,
    limit: SearchLimit,
    offset: u32,
}

/// Unvalidated operation input. The owner validates this after attributed admission begins.
/// This is not a transport envelope and contains no authentication or Task fields.
#[derive(Debug, Clone, Default)]
pub struct SearchInput {
    /// Optional caller query, preserved exactly when accepted.
    pub query: Option<String>,
    /// Optional Provider filter: a namespace, such as `github`, or `coral`.
    pub provider: Option<String>,
    /// Optional requested page size.
    pub limit: Option<u16>,
    /// Optional count of ranked results to skip: the `next_offset` of an
    /// earlier page. Absent for the first page.
    pub offset: Option<u32>,
}

impl TryFrom<SearchInput> for SearchRequest {
    type Error = Error;
    fn try_from(input: SearchInput) -> Result<Self, Error> {
        let mut request = match input.query {
            Some(query) => Self::query(SearchQuery::parse(&query)?),
            None => Self::list(),
        };
        request = request.with_providers(ProviderFilter::new(input.provider)?);
        if let Some(limit) = input.limit {
            request = request.with_limit(SearchLimit::parse(limit)?);
        }
        if let Some(offset) = input.offset {
            request = request.with_offset(offset);
        }
        Ok(request)
    }
}

/// Parses the `provider` filter: a Provider Namespace, such as `github`, or
/// `coral` for the Built-in Provider. A complete `coral.providers` Call
/// Root is rejected with the namespace to use instead.
fn provider_filter(value: &str) -> Result<ProviderCallRoot, Error> {
    match ProviderCallRoot::parse(value) {
        Ok(ProviderCallRoot::Builtin) => Ok(ProviderCallRoot::Builtin),
        Ok(ProviderCallRoot::Owner(namespace)) => Err(Error::InvalidInput(format!(
            "provider must be a Provider namespace, such as `{}`",
            namespace.as_str()
        ))),
        // The Private and Shared Providers' catalogs hold no Tool for a
        // filter to keep.
        Ok(ProviderCallRoot::Private | ProviderCallRoot::Shared) | Err(_) => {
            ProviderNamespace::parse(value)
                .map(ProviderCallRoot::Owner)
                .map_err(|_invalid_namespace| {
                    Error::InvalidInput("invalid Provider namespace".into())
                })
        }
    }
}

/// The `provider` filter value that selects the Provider at this Call Root:
/// its namespace, such as `github`, or `coral` for the Built-in Provider.
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
    pub fn with_provider(mut self, provider: &ProviderCallRoot) -> Self {
        self.provider = ProviderFilter::one(provider);
        self
    }

    /// Restricts results to the Tools of the Providers the filter names.
    #[must_use]
    pub fn with_providers(mut self, provider: ProviderFilter) -> Self {
        self.provider = provider;
        self
    }

    /// Applies one validated result-count bound.
    #[must_use]
    pub const fn with_limit(mut self, limit: SearchLimit) -> Self {
        self.limit = limit;
        self
    }

    /// Starts the page after this many ranked results. An offset past the
    /// end gives an empty final page.
    #[must_use]
    pub const fn with_offset(mut self, offset: u32) -> Self {
        self.offset = offset;
        self
    }

    /// Returns the optional exact query.
    #[must_use]
    pub const fn search_query(&self) -> Option<&SearchQuery> {
        self.query.as_ref()
    }

    /// Returns the Provider filter, empty when it leaves every Provider.
    #[must_use]
    pub const fn provider(&self) -> &ProviderFilter {
        &self.provider
    }

    /// Returns the page's hit count limit.
    #[must_use]
    pub const fn limit(&self) -> SearchLimit {
        self.limit
    }

    /// Returns how many ranked results the page skips; 0 for the first page.
    #[must_use]
    pub const fn offset(&self) -> u32 {
        self.offset
    }
}

/// Complete public JavaScript path for one Provider Tool, Private Tool or
/// Shared Tool.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CallPath(String);

impl CallPath {
    /// Parses one complete Call Path: a Provider Call Root, `coral.private`
    /// or `coral.shared`, followed by the relative path.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] unless every path member is a safe
    /// JavaScript identifier under `coral` with a root and a relative path.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if ProviderCallRoot::split_call_path(value).is_none()
            || value.split('.').any(|segment| !safe_path_segment(segment))
        {
            return Err(Error::InvalidInput(
                "Call Path must be a complete safe JavaScript path under coral, coral.providers, coral.private or coral.shared"
                    .to_string(),
            ));
        }
        Ok(Self(value.to_string()))
    }

    pub(crate) fn tool(call_root: &ProviderCallRoot, relative_path: &str) -> Result<Self, Error> {
        Self::parse(&format!("{}.{relative_path}", call_root.as_str()))
    }

    pub(crate) fn authored_tool(path: &coral_tools::AuthoredToolPath) -> Result<Self, Error> {
        Self::parse(&path.call_path())
    }

    /// Returns the path of the Tool of `kind` this Call Path names, under
    /// the kind's Call Root. `None` for another kind's Call Path, and for a
    /// path outside the authoring bounds, which names no Authored Tool.
    pub(crate) fn authored_tool_path(
        &self,
        kind: AuthoredToolKind,
    ) -> Option<coral_tools::AuthoredToolPath> {
        coral_tools::AuthoredToolPath::parse(kind, kind.relative_path(&self.0)?).ok()
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
    /// One current Principal-owned Private Tool.
    PrivateTool,
    /// One live Shared Tool of the Workspace.
    SharedTool,
}

impl EntityKind {
    /// The kind's stable name, as search results and telemetry spell it.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderTool => "provider_tool",
            Self::PrivateTool => "private_tool",
            Self::SharedTool => "shared_tool",
        }
    }

    /// The Search Entity kind of an Authored Tool of `kind`.
    pub(crate) const fn authored(kind: AuthoredToolKind) -> Self {
        match kind {
            AuthoredToolKind::Private => Self::PrivateTool,
            AuthoredToolKind::Shared => Self::SharedTool,
        }
    }
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

/// Complete Tool Description of one Authored Tool the caller can discover,
/// one of its own Private Tools or one of the Workspace's Shared Tools: its
/// TypeScript authoring contract, its kind's Provider, its current revision
/// and whether a person must approve each call. It names no author.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoredToolAuthoringDescription {
    kind: AuthoredToolKind,
    call_path: CallPath,
    description: String,
    typescript_declaration: String,
    provider_id: ProviderId,
    provider_display_name: String,
    revision: u64,
    approval_requirement: ApprovalRequirement,
}

impl AuthoredToolAuthoringDescription {
    #[expect(
        clippy::too_many_arguments,
        reason = "one description is built once, where its retriever describes it"
    )]
    pub(crate) const fn new(
        kind: AuthoredToolKind,
        call_path: CallPath,
        description: String,
        typescript_declaration: String,
        provider_id: ProviderId,
        provider_display_name: String,
        revision: u64,
        approval_requirement: ApprovalRequirement,
    ) -> Self {
        Self {
            kind,
            call_path,
            description,
            typescript_declaration,
            provider_id,
            provider_display_name,
            revision,
            approval_requirement,
        }
    }

    /// Returns whether this is a Private Tool or a Shared Tool.
    #[must_use]
    pub const fn kind(&self) -> AuthoredToolKind {
        self.kind
    }

    /// Returns the complete callable path.
    #[must_use]
    pub const fn call_path(&self) -> &CallPath {
        &self.call_path
    }

    /// Returns the exact explicit description.
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

    /// Returns the identity of the kind's Provider in the Workspace: its
    /// Private Provider or its Shared Provider.
    #[must_use]
    pub const fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }

    /// Returns that Provider's display name.
    #[must_use]
    pub fn provider_display_name(&self) -> &str {
        &self.provider_display_name
    }

    /// Returns a Private Tool's definition revision, or a Shared Tool's
    /// current revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns whether current policy requires an Invocation Approval.
    #[must_use]
    pub const fn approval_requirement(&self) -> ApprovalRequirement {
        self.approval_requirement
    }
}

/// One complete current TypeScript Authoring Description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthoringDescription {
    /// A visible Tool contract.
    Tool(ToolAuthoringDescription),
    /// The authenticated Principal's own current Private Tool, or a live
    /// Shared Tool of the Workspace.
    AuthoredTool(AuthoredToolAuthoringDescription),
}

impl AuthoringDescription {
    /// Returns the callable kind.
    #[must_use]
    pub const fn kind(&self) -> EntityKind {
        match self {
            Self::Tool(_) => EntityKind::ProviderTool,
            Self::AuthoredTool(description) => EntityKind::authored(description.kind()),
        }
    }

    /// Returns the complete Call Path.
    #[must_use]
    pub const fn call_path(&self) -> &CallPath {
        match self {
            Self::Tool(description) => description.call_path(),
            Self::AuthoredTool(description) => description.call_path(),
        }
    }

    /// Returns the safe complete authoring description.
    #[must_use]
    pub fn description(&self) -> &str {
        match self {
            Self::Tool(description) => description.description(),
            Self::AuthoredTool(description) => description.description(),
        }
    }

    /// Returns the TypeScript declaration.
    #[must_use]
    pub fn typescript_declaration(&self) -> &str {
        match self {
            Self::Tool(description) => description.typescript_declaration(),
            Self::AuthoredTool(description) => description.typescript_declaration(),
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
    signature: SignatureLine,
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
    pub(crate) signature: SignatureLine,
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

    pub(crate) fn from_authored_tool(
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
            signature: text.signature,
            description_snippet: text.description_snippet,
            guidance_snippet: text.guidance_snippet,
            matched_tags: text.matched_tags,
            evidence,
            tool,
        }
    }

    /// Returns the Search Entity kind.
    #[must_use]
    pub fn kind(&self) -> EntityKind {
        if self.tool.is_some() {
            return EntityKind::ProviderTool;
        }
        // The Call Path names the kind of an Authored Tool.
        match AuthoredToolKind::of_call_path(self.call_path.as_str()) {
            Some(AuthoredToolKind::Shared) => EntityKind::SharedTool,
            Some(AuthoredToolKind::Private) | None => EntityKind::PrivateTool,
        }
    }

    /// Returns the complete callable path.
    #[must_use]
    pub const fn call_path(&self) -> &CallPath {
        &self.call_path
    }

    /// Returns the bounded signature line.
    #[must_use]
    pub fn signature_line(&self) -> &str {
        &self.signature.text
    }

    /// Returns the signature line's callable and input, without its result.
    pub(crate) fn signature_input(&self) -> &str {
        self.signature.input()
    }

    /// Returns whether the signature line is complete enough to call from:
    /// it has no cut marker, and its input names no type it does not show.
    #[must_use]
    pub const fn complete(&self) -> bool {
        self.signature.complete
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
            self.signature.text.as_str(),
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
    /// Tools; each Private or Shared Tool the Principal can discover for
    /// its own kind.
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
    next_offset: Option<u32>,
    retriever_statuses: Vec<RetrieverStatus>,
}

impl SearchPage {
    pub(crate) const fn new(
        hits: Vec<SearchHit>,
        next_offset: Option<u32>,
        retriever_statuses: Vec<RetrieverStatus>,
    ) -> Self {
        Self {
            hits,
            next_offset,
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

    /// Returns the offset of the next page while the current result universe
    /// has another candidate.
    #[must_use]
    pub const fn next_offset(&self) -> Option<u32> {
        self.next_offset
    }
}

/// Exact current result of describing one complete Call Path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactAuthoringDescription {
    authoring: AuthoringDescription,
    authored_tool_detail: Option<ExactAuthoredToolDetail>,
}

impl ExactAuthoringDescription {
    pub(crate) const fn tool(authoring: ToolAuthoringDescription) -> Self {
        Self {
            authoring: AuthoringDescription::Tool(authoring),
            authored_tool_detail: None,
        }
    }

    pub(crate) const fn authored_tool(
        authoring: AuthoredToolAuthoringDescription,
        detail: ExactAuthoredToolDetail,
    ) -> Self {
        Self {
            authoring: AuthoringDescription::AuthoredTool(authoring),
            authored_tool_detail: Some(detail),
        }
    }

    /// The same description, with its declaration abridged, or cut to the
    /// `types` it names. A description cut to `types` leaves out an Authored
    /// Tool's source and diagnostics, which the caller already has.
    pub(crate) fn abridge(mut self, types: &[String]) -> Result<Self, crate::Error> {
        if !types.is_empty() {
            self.authored_tool_detail = None;
        }
        let kind = self.authoring.kind();
        let declaration = match &mut self.authoring {
            AuthoringDescription::Tool(description) => &mut description.typescript_declaration,
            AuthoringDescription::AuthoredTool(description) => {
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

    /// Returns the source and stored diagnostics of an exact Private or
    /// Shared Tool.
    #[must_use]
    pub const fn authored_tool_detail(&self) -> Option<&ExactAuthoredToolDetail> {
        self.authored_tool_detail.as_ref()
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
