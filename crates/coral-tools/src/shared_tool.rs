//! Shared Tools: the Workspace-owned Tools at `coral.shared` that a
//! Workspace Owner publishes (ADR 0034).
//!
//! [`SharedToolModule`] owns publishing, removal and the reads of every
//! Principal in the Workspace. A publish compiles the reviewed source outside
//! its write transaction, then holds Owner authority, the source it copies
//! and the current revision at its path, and writes one immutable revision,
//! its dependencies and one lifecycle Event together. Its author is
//! attribution only: no access removal reads or writes these rows.
//!
//! A Shared Tool has two names. Its path names it while it is live: a
//! publish, a removal and the reads of every Principal go by path. Its
//! [`SharedToolId`] names it for good: a removal frees the path and keeps the
//! revisions, so the history (every revision and every lifecycle Event) goes
//! by identity. Member Users read the history; Agents do not.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use coral_catalog::{AuthoredToolKind, ToolAccessState};
use coral_db::{Database, Transaction};
use coral_run_executor::CompilerClient;
use coral_workspace::{
    AuthenticatedPrincipal, AuthoritySource, ChangeContext, Principal, PrincipalKind, UserId,
    WorkspaceAction, WorkspaceEvidence, WorkspaceId, WorkspaceModule, WorkspaceRole,
};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::authoring::{
    Authoring, PreparedAuthoredTool, Typing, missing_linked_tool, now_unix_nanos,
    tool_paths_overlap, v8_compiler,
};
use crate::tool_typing::ToolDeclarationReader;
use crate::types::AuthoredToolPath;
use crate::types::{
    AuthoredToolDeclaration, AuthoredToolDeclarationFieldProjection, AuthoredToolDescription,
    AuthoredToolDiagnostic, AuthoredToolSource, PrivateToolId, PrivateToolPath, authored_tool_path,
};
use crate::{Error, repository, shared_tool_repository};

/// The most Shared Tools, or lifecycle Events of them, one page lists.
pub const SHARED_TOOL_PAGE_MAX_SIZE: u32 = 100;

/// Stable Coral-generated identity of one Shared Tool, kept across its
/// revisions. It is the Tool Key of the Shared Tool in the Shared Provider.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SharedToolId(Uuid);

impl SharedToolId {
    pub(crate) fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    /// Parses a Shared Tool identity at an Adapter boundary.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] unless the value is a canonical UUID.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let id = Uuid::parse_str(value)
            .map_err(|_error| Error::InvalidInput("Shared Tool id must be a UUID".to_string()))?;
        if id.hyphenated().to_string() != value {
            return Err(Error::InvalidInput(
                "Shared Tool id must use canonical lowercase UUID form".to_string(),
            ));
        }
        Ok(Self(id))
    }

    /// The Shared Tool's Tool Key in the Shared Provider, which an exact
    /// rule names.
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

impl fmt::Display for SharedToolId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0.hyphenated())
    }
}

/// A UUID Coral generated, or one parsed in its canonical form: it holds
/// no text.
impl coral_telemetry::safe::Identifier for SharedToolId {}

/// The dotted JavaScript path relative to `coral.shared` that names one
/// live Shared Tool in its Workspace. Its bounds and segments follow the
/// rules of a Private Tool Path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SharedToolPath(String);

impl SharedToolPath {
    /// Validates the fixed path bounds and safe JavaScript segments.
    ///
    /// # Errors
    ///
    /// Rejects empty paths, more than sixteen segments, more than 256 UTF-8
    /// bytes, and invalid or reserved JavaScript identifier segments.
    pub fn parse(value: &str) -> Result<Self, Error> {
        authored_tool_path(value, "Shared Tool Path").map(Self)
    }

    /// Returns the complete dotted path relative to `coral.shared`.
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

/// What a Shared Tool Publish copies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SharedToolOrigin {
    /// A current Private Tool: the publishing Owner's own or an Agent's.
    PrivateTool {
        /// The Principal whose Private Tool it is.
        owner: Principal,
        /// Its path relative to `coral.private`.
        path: PrivateToolPath,
    },
    /// An earlier revision of the Shared Tool at the publish's path: a
    /// restore.
    Revision(u64),
}

/// What one Shared Tool Revision copied, as it recorded it: provenance
/// only, since the revision keeps its own content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SharedToolRevisionOrigin {
    /// The Private Tool it copied, which may since have changed or gone.
    PrivateTool(PrivateToolId),
    /// The earlier revision of the same Shared Tool that it restored.
    Revision(u64),
}

/// One Workspace Owner's request about the content they mean to publish as
/// a new revision of the Shared Tool at `path`: its review, or the publish
/// of what that review showed.
#[derive(Debug, Clone)]
pub struct SharedToolPublish {
    path: SharedToolPath,
    origin: SharedToolOrigin,
    description: AuthoredToolDescription,
    source: AuthoredToolSource,
    default_access_state: ToolAccessState,
    expected_revision: u64,
    /// The declaration the Owner reviewed. `None` asks for the review only.
    reviewed_declaration: Option<String>,
}

impl SharedToolPublish {
    /// Asks for the review of a publish of `source` and `description` from
    /// `origin`, at `path`, with `default_access_state` as its default Tool
    /// Access State. `expected_revision` is the revision the Owner expects
    /// to be current at `path`: zero to create. Coral checks the publish
    /// and writes nothing.
    #[must_use]
    pub const fn review(
        path: SharedToolPath,
        origin: SharedToolOrigin,
        description: AuthoredToolDescription,
        source: AuthoredToolSource,
        default_access_state: ToolAccessState,
        expected_revision: u64,
    ) -> Self {
        Self {
            path,
            origin,
            description,
            source,
            default_access_state,
            expected_revision,
            reviewed_declaration: None,
        }
    }

    /// Turns the review into a publish of its content: the Owner approves
    /// `declaration`, the one their review showed. Coral emits the
    /// declaration again at the publish, and refuses the publish when it is
    /// not `declaration`.
    #[must_use]
    pub fn approve(mut self, declaration: impl Into<String>) -> Self {
        self.reviewed_declaration = Some(declaration.into());
        self
    }

    /// Returns the path relative to `coral.shared`.
    #[must_use]
    pub const fn path(&self) -> &SharedToolPath {
        &self.path
    }

    fn admits(&self, current: u64) -> Result<(), Error> {
        if current == self.expected_revision {
            Ok(())
        } else {
            Err(Error::RevisionConflict {
                kind: AuthoredToolKind::Shared,
                current,
            })
        }
    }
}

/// One immutable, numbered publication of a Shared Tool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedToolRevision {
    pub(crate) shared_tool_id: SharedToolId,
    pub(crate) revision: u64,
    pub(crate) description: AuthoredToolDescription,
    pub(crate) source: AuthoredToolSource,
    pub(crate) declaration: AuthoredToolDeclaration,
    pub(crate) declaration_field_projection: AuthoredToolDeclarationFieldProjection,
    pub(crate) diagnostics: Vec<AuthoredToolDiagnostic>,
    pub(crate) diagnostics_truncated: bool,
    pub(crate) content_hash: String,
    pub(crate) default_access_state: ToolAccessState,
    pub(crate) published_by: UserId,
    pub(crate) origin: SharedToolRevisionOrigin,
    pub(crate) published_at_unix_nanos: i64,
}

impl SharedToolRevision {
    /// Returns the Shared Tool this revision belongs to.
    #[must_use]
    pub const fn shared_tool_id(&self) -> &SharedToolId {
        &self.shared_tool_id
    }

    /// Returns the revision number, from 1.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the exact published description.
    #[must_use]
    pub const fn description(&self) -> &AuthoredToolDescription {
        &self.description
    }

    /// Returns the exact published source.
    #[must_use]
    pub const fn source(&self) -> &AuthoredToolSource {
        &self.source
    }

    /// Returns the declaration emitted from the source.
    #[must_use]
    pub const fn declaration(&self) -> &AuthoredToolDeclaration {
        &self.declaration
    }

    /// Returns the searchable declaration fields, or why they are missing.
    #[must_use]
    pub const fn declaration_field_projection(&self) -> &AuthoredToolDeclarationFieldProjection {
        &self.declaration_field_projection
    }

    /// Returns the advisory diagnostics retained with the revision.
    #[must_use]
    pub fn diagnostics(&self) -> &[AuthoredToolDiagnostic] {
        &self.diagnostics
    }

    /// Returns whether diagnostics beyond the retention bound were omitted.
    #[must_use]
    pub const fn diagnostics_truncated(&self) -> bool {
        self.diagnostics_truncated
    }

    /// Returns the lowercase hex SHA-256 of the description and source.
    #[must_use]
    pub fn content_hash(&self) -> &str {
        &self.content_hash
    }

    /// Returns the default Tool Access State the Owner chose at publish.
    #[must_use]
    pub const fn default_access_state(&self) -> ToolAccessState {
        self.default_access_state
    }

    /// Returns the Workspace Owner who published it: attribution only.
    #[must_use]
    pub const fn published_by(&self) -> &UserId {
        &self.published_by
    }

    /// Returns what it copied.
    #[must_use]
    pub const fn origin(&self) -> &SharedToolRevisionOrigin {
        &self.origin
    }

    /// Returns when it was published.
    #[must_use]
    pub const fn published_at_unix_nanos(&self) -> i64 {
        self.published_at_unix_nanos
    }
}

/// One live Shared Tool with its current revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedTool {
    pub(crate) id: SharedToolId,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) path: SharedToolPath,
    pub(crate) created_by: UserId,
    pub(crate) created_at_unix_nanos: i64,
    pub(crate) current: SharedToolRevision,
}

impl SharedTool {
    /// Returns its stable identity.
    #[must_use]
    pub const fn id(&self) -> &SharedToolId {
        &self.id
    }

    /// Returns its Workspace.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    /// Returns its path relative to `coral.shared`.
    #[must_use]
    pub const fn path(&self) -> &SharedToolPath {
        &self.path
    }

    /// Returns the Workspace Owner who first published it: attribution only.
    #[must_use]
    pub const fn created_by(&self) -> &UserId {
        &self.created_by
    }

    /// Returns when it was first published.
    #[must_use]
    pub const fn created_at_unix_nanos(&self) -> i64 {
        self.created_at_unix_nanos
    }

    /// Returns its current revision.
    #[must_use]
    pub const fn current(&self) -> &SharedToolRevision {
        &self.current
    }
}

/// A Shared Tool one revision calls, at the revision current when the
/// revision was published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedToolDependency {
    pub(crate) id: SharedToolId,
    pub(crate) path: SharedToolPath,
    pub(crate) revision: u64,
}

impl SharedToolDependency {
    /// Returns the Shared Tool it calls.
    #[must_use]
    pub const fn id(&self) -> &SharedToolId {
        &self.id
    }

    /// Returns its path relative to `coral.shared`.
    #[must_use]
    pub const fn path(&self) -> &SharedToolPath {
        &self.path
    }

    /// Returns the revision that was current.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// What an Owner reviews before a publish: what would change and what the
/// source calls. Coral computes it from the exact content of the request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedToolReview {
    previous: Option<SharedToolRevision>,
    declaration: AuthoredToolDeclaration,
    diagnostics: Vec<AuthoredToolDiagnostic>,
    diagnostics_truncated: bool,
    called_tools: Vec<String>,
    shared_tools: Vec<SharedToolDependency>,
    runtime_built_paths: Vec<String>,
}

impl SharedToolReview {
    /// Returns the revision current at the path, the other side of the
    /// source and declaration diffs; `None` when the publish creates the
    /// Shared Tool.
    #[must_use]
    pub const fn previous(&self) -> Option<&SharedToolRevision> {
        self.previous.as_ref()
    }

    /// Returns the declaration the publish would emit.
    #[must_use]
    pub const fn declaration(&self) -> &AuthoredToolDeclaration {
        &self.declaration
    }

    /// Returns the advisory diagnostics of the reviewed source.
    #[must_use]
    pub fn diagnostics(&self) -> &[AuthoredToolDiagnostic] {
        &self.diagnostics
    }

    /// Returns whether diagnostics beyond the retention bound were omitted.
    #[must_use]
    pub const fn diagnostics_truncated(&self) -> bool {
        self.diagnostics_truncated
    }

    /// Returns every Call Path the source calls by a literal path, in path
    /// order: Provider and Built-in Tools, the caller's Private Tools and
    /// Shared Tools.
    #[must_use]
    pub fn called_tools(&self) -> &[String] {
        &self.called_tools
    }

    /// Returns the Shared Tools the source calls, with their current
    /// revisions.
    #[must_use]
    pub fn shared_tools(&self) -> &[SharedToolDependency] {
        &self.shared_tools
    }

    /// Returns the literal start of each Tool path the source builds at
    /// runtime: the Tools it calls there are known only when it runs, so
    /// [`Self::called_tools`] may be incomplete.
    #[must_use]
    pub fn runtime_built_paths(&self) -> &[String] {
        &self.runtime_built_paths
    }
}

/// A publish's outcome: the review, and the Shared Tool as it now is unless
/// the request was a review.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedToolPublication {
    review: SharedToolReview,
    published: Option<SharedTool>,
}

impl SharedToolPublication {
    /// Returns the review of the published content.
    #[must_use]
    pub const fn review(&self) -> &SharedToolReview {
        &self.review
    }

    /// Returns the Shared Tool with its new current revision; `None` for a
    /// review.
    #[must_use]
    pub const fn published(&self) -> Option<&SharedTool> {
        self.published.as_ref()
    }
}

/// One live Shared Tool without its source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedToolSummary {
    pub(crate) id: SharedToolId,
    pub(crate) path: SharedToolPath,
    pub(crate) description: AuthoredToolDescription,
    pub(crate) current_revision: u64,
    pub(crate) published_at_unix_nanos: i64,
}

impl SharedToolSummary {
    /// Returns its stable identity.
    #[must_use]
    pub const fn id(&self) -> &SharedToolId {
        &self.id
    }

    /// Returns its path relative to `coral.shared`.
    #[must_use]
    pub const fn path(&self) -> &SharedToolPath {
        &self.path
    }

    /// Returns its current description.
    #[must_use]
    pub const fn description(&self) -> &AuthoredToolDescription {
        &self.description
    }

    /// Returns its current revision number.
    #[must_use]
    pub const fn current_revision(&self) -> u64 {
        self.current_revision
    }

    /// Returns when its current revision was published.
    #[must_use]
    pub const fn published_at_unix_nanos(&self) -> i64 {
        self.published_at_unix_nanos
    }
}

/// One page of live Shared Tools in path order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedToolPage {
    pub(crate) summaries: Vec<SharedToolSummary>,
    pub(crate) next_after: Option<SharedToolPath>,
}

impl SharedToolPage {
    /// Returns the Shared Tools on this page.
    #[must_use]
    pub fn summaries(&self) -> &[SharedToolSummary] {
        &self.summaries
    }

    /// Returns the path the next page follows, when there is one.
    #[must_use]
    pub const fn next_after(&self) -> Option<&SharedToolPath> {
        self.next_after.as_ref()
    }
}

/// A live Shared Tool as a policy listing shows it: its Tool Key, its path
/// and the default Tool Access State of its current revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedToolDefault {
    pub(crate) key: coral_catalog::ToolKey,
    pub(crate) path: SharedToolPath,
    pub(crate) default_access_state: ToolAccessState,
}

impl SharedToolDefault {
    /// Returns the Tool Key in the Shared Provider.
    #[must_use]
    pub const fn key(&self) -> &coral_catalog::ToolKey {
        &self.key
    }

    /// Returns the path relative to `coral.shared`.
    #[must_use]
    pub const fn path(&self) -> &SharedToolPath {
        &self.path
    }

    /// Returns the default the Owner chose when publishing the current
    /// revision.
    #[must_use]
    pub const fn default_access_state(&self) -> ToolAccessState {
        self.default_access_state
    }
}

/// What one lifecycle Event of a Shared Tool records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedToolLifecycleAction {
    /// A publish made a new revision current.
    Published,
    /// A removal tombstoned the Shared Tool and freed its path.
    Removed,
}

impl SharedToolLifecycleAction {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Published => "shared_tool.published",
            Self::Removed => "shared_tool.removed",
        }
    }

    pub(crate) fn from_storage(value: &str) -> Result<Self, Error> {
        match value {
            "shared_tool.published" => Ok(Self::Published),
            "shared_tool.removed" => Ok(Self::Removed),
            _ => Err(Error::CorruptData(
                "Shared Tool lifecycle Event has an unrecognized action".to_string(),
            )),
        }
    }
}

/// Stable identity of one lifecycle Event of a Shared Tool. A page of
/// Events continues after the Event it names.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SharedToolLifecycleEventId(Uuid);

impl SharedToolLifecycleEventId {
    pub(crate) fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    /// Parses an Event identity at an Adapter boundary.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] unless the value is a canonical UUID.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let invalid = || Error::InvalidInput("Shared Tool Event id must be a UUID".to_string());
        let id = Uuid::parse_str(value).map_err(|_error| invalid())?;
        if id.hyphenated().to_string() != value {
            return Err(invalid());
        }
        Ok(Self(id))
    }
}

impl fmt::Display for SharedToolLifecycleEventId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0.hyphenated())
    }
}

/// Safe immutable evidence of one publish or removal of a Shared Tool: who
/// did it, to which revision, and when. It never holds source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedToolLifecycleEvent {
    pub(crate) id: SharedToolLifecycleEventId,
    pub(crate) action: SharedToolLifecycleAction,
    pub(crate) actor: UserId,
    pub(crate) shared_tool_id: SharedToolId,
    pub(crate) path: SharedToolPath,
    pub(crate) revision: u64,
    pub(crate) content_hash: String,
    pub(crate) occurred_at_unix_nanos: i64,
}

impl SharedToolLifecycleEvent {
    /// Returns its stable identity.
    #[must_use]
    pub const fn id(&self) -> &SharedToolLifecycleEventId {
        &self.id
    }

    /// Returns what happened.
    #[must_use]
    pub const fn action(&self) -> SharedToolLifecycleAction {
        self.action
    }

    /// Returns the Workspace Owner who published or removed.
    #[must_use]
    pub const fn actor(&self) -> &UserId {
        &self.actor
    }

    /// Returns the Shared Tool, which may since have been removed.
    #[must_use]
    pub const fn shared_tool_id(&self) -> &SharedToolId {
        &self.shared_tool_id
    }

    /// Returns the path the Shared Tool had. After its removal, another
    /// Shared Tool may have it.
    #[must_use]
    pub const fn path(&self) -> &SharedToolPath {
        &self.path
    }

    /// Returns the revision a publish made current, or the revision that
    /// was current when a removal tombstoned the Shared Tool.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the content hash of that revision.
    #[must_use]
    pub fn content_hash(&self) -> &str {
        &self.content_hash
    }

    /// Returns when it happened.
    #[must_use]
    pub const fn occurred_at_unix_nanos(&self) -> i64 {
        self.occurred_at_unix_nanos
    }
}

/// One page of lifecycle Events, newest first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedToolLifecycleEventPage {
    pub(crate) events: Vec<SharedToolLifecycleEvent>,
    pub(crate) next_after: Option<SharedToolLifecycleEventId>,
}

impl SharedToolLifecycleEventPage {
    /// Returns the Events on this page.
    #[must_use]
    pub fn events(&self) -> &[SharedToolLifecycleEvent] {
        &self.events
    }

    /// Returns the Event the next page follows, when there is one.
    #[must_use]
    pub const fn next_after(&self) -> Option<&SharedToolLifecycleEventId> {
        self.next_after.as_ref()
    }
}

/// The Workspace's Shared Tools: Owner publishing and removal, reads for
/// every Principal in the Workspace, and their history for Member Users.
#[derive(Clone)]
pub struct SharedToolModule {
    database: Arc<Database>,
    workspaces: WorkspaceModule,
    authoring: Authoring,
}

impl SharedToolModule {
    /// Builds the Module with a warm compiler of its own, within the
    /// deployment's compiler heap budget. A composed process shares its
    /// one compiler through [`Self::with_compiler_client`] instead.
    ///
    /// # Errors
    ///
    /// Returns an initialization error when the bounded compiler cannot start.
    pub fn new(
        database: Arc<Database>,
        workspaces: WorkspaceModule,
        isolate_limits: coral_v8::IsolateLimits,
        compilation_timeout: Duration,
    ) -> Result<Self, Error> {
        let compiler = coral_run_executor::InProcessCompiler::new(isolate_limits)
            .map_err(|error| Error::CompilerUnavailable(error.to_string()))?;
        Ok(Self::with_compiler_client(
            database,
            workspaces,
            Arc::new(compiler),
            compilation_timeout,
        ))
    }

    /// Builds the Module as [`Self::new`] does, except that it compiles
    /// through `compiler`, the process's one warm compiler.
    #[must_use]
    pub fn with_compiler_client(
        database: Arc<Database>,
        workspaces: WorkspaceModule,
        compiler: Arc<dyn CompilerClient>,
        compilation_timeout: Duration,
    ) -> Self {
        Self {
            authoring: Authoring::new(database.clone(), v8_compiler(compiler), compilation_timeout),
            database,
            workspaces,
        }
    }

    /// Publishes the exact content the Owner reviewed as the next revision
    /// of the Shared Tool at its path, and makes it current; or, for a
    /// [`SharedToolPublish::review`], returns its review and writes nothing.
    ///
    /// The source is compiled outside the write transaction, and never
    /// executed. The declaration it emits depends on the Tools it names, so
    /// one of them may have changed it since the review: a publish stores
    /// the declaration only when it is the one the Owner reviewed. The
    /// source is typed as a Private Tool save types it: against the
    /// current Shared Tools it names and, through `tools`, against the
    /// Provider and Built-in Tools it names that the publishing Owner can
    /// discover (ADR 0020). Without `tools`, those stay untyped.
    /// `coral.private` always stays untyped, because it is each caller's
    /// own. The write transaction holds the Owner's authority, the source
    /// the publish copies and the current revision at the path, then writes
    /// the revision, the Shared Tools it calls and one lifecycle Event
    /// together. The Private Tool it copies stays as it was.
    ///
    /// This publish does not notify Universal Search: coral-search depends on
    /// this crate. The composing caller sends the Shared Tool's id to
    /// `SearchIndex::sync_authored_tool` after the commit, and supplies
    /// `tools`, as `coral_administration::SharedToolWorkflow` does.
    ///
    /// # Errors
    ///
    /// - [`Error::PermissionDenied`] unless the actor is a Workspace Owner by
    ///   Workspace Membership.
    /// - [`Error::SharedToolSourceUnreadable`] when the origin is another
    ///   User's Private Tool, a Private Tool that is gone, or a revision that
    ///   never existed.
    /// - [`Error::SharedToolRestoreDiffers`] when the origin is a revision
    ///   and the source or the description is not that revision's.
    /// - [`Error::RevisionConflict`] when the current revision at
    ///   the path is not the expected one.
    /// - [`Error::PathConflict`] when the path overlaps another
    ///   live Shared Tool's namespace.
    /// - An authoring failure when the source cannot become a Tool.
    /// - [`Error::SharedToolDeclarationChanged`] when the declaration the
    ///   source emits is not the one the Owner reviewed.
    ///
    /// Failure changes nothing.
    pub async fn publish(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        publish: SharedToolPublish,
        tools: Option<&dyn ToolDeclarationReader>,
    ) -> Result<SharedToolPublication, Error> {
        let prepared = self.prepare(context, workspace_id, &publish, tools).await?;
        let Some(reviewed) = &publish.reviewed_declaration else {
            return Ok(SharedToolPublication {
                review: prepared.review,
                published: None,
            });
        };
        // The transaction stores the declaration compiled here, so this is
        // the one to compare with what the Owner saw.
        if prepared.compiled.declaration.as_str() != reviewed {
            return Err(Error::SharedToolDeclarationChanged);
        }
        let mut transaction = self.database.begin().await?;
        let published = self
            .publish_in_transaction(
                &mut transaction,
                context,
                workspace_id,
                &publish,
                prepared.compiled,
            )
            .await?;
        transaction.commit().await?;
        Ok(SharedToolPublication {
            review: prepared.review,
            published: Some(published),
        })
    }

    /// Publishes `review` as an Owner who approves the review they are
    /// shown: reviews it, then publishes it with the declaration that
    /// review returned. For tests that are not about the review.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`Self::publish`].
    #[cfg(any(test, feature = "test-fixtures"))]
    pub async fn publish_as_reviewed(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        review: SharedToolPublish,
        tools: Option<&dyn ToolDeclarationReader>,
    ) -> Result<SharedToolPublication, Error> {
        let shown = self
            .publish(context, workspace_id, review.clone(), tools)
            .await?;
        let declaration = shown.review().declaration().as_str().to_owned();
        self.publish(context, workspace_id, review.approve(declaration), tools)
            .await
    }

    /// Removes the live Shared Tool at `path` and returns its identity,
    /// which still names its revisions and its history.
    ///
    /// The removal leaves a tombstone: the path is free for another Shared
    /// Tool, and every revision is kept. A Program or a Tool that still
    /// calls the path fails at its next admission as any path that names
    /// no Tool, and a Run already accepted keeps the revision it linked.
    /// The write transaction holds the Owner's authority and the Shared
    /// Tool, checks that no other Shared Tool calls it, then writes the
    /// tombstone and one lifecycle Event together.
    ///
    /// This removal commits here, for tests: it leaves the Shared Tool's
    /// exact rule and does not notify Universal Search. coral-policy and
    /// coral-search depend on this crate, so neither call can come from
    /// here. Production removes through
    /// `coral_administration::SharedToolWorkflow`, which joins
    /// [`Self::remove_in_transaction`].
    ///
    /// # Errors
    ///
    /// - [`Error::PermissionDenied`] unless the actor is a Workspace Owner by
    ///   Workspace Membership.
    /// - [`Error::NotFound`] when no live Shared Tool is at the path.
    /// - [`Error::RevisionConflict`] when its current revision is not
    ///   `expected_revision`.
    /// - [`Error::SharedToolHasDependents`] while the current revision of
    ///   another live Shared Tool calls it, naming that Shared Tool.
    ///
    /// Failure changes nothing.
    #[cfg(any(test, feature = "test-fixtures"))]
    pub async fn remove(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        path: &SharedToolPath,
        expected_revision: u64,
    ) -> Result<SharedToolId, Error> {
        let mut transaction = self.database.begin().await?;
        let removed = self
            .remove_in_transaction(
                &mut transaction,
                context,
                workspace_id,
                path,
                expected_revision,
            )
            .await?;
        transaction.commit().await?;
        Ok(removed)
    }

    /// Returns the live Shared Tool at `path` with its current revision, to
    /// any Principal in the Workspace.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] when no live Shared Tool is
    /// there or the caller cannot inspect the Workspace.
    pub async fn get(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        path: &SharedToolPath,
    ) -> Result<SharedTool, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.authorize_inspection(&mut transaction, actor, workspace_id)
            .await?;
        let tool =
            shared_tool_repository::find_live_tool(&mut transaction, workspace_id, path).await?;
        transaction.commit().await?;
        tool.ok_or(Error::NotFound(AuthoredToolKind::Shared))
    }

    /// Lists one page of the Workspace's live Shared Tools in path order
    /// after `after`, to any Principal in the Workspace.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] unless `page_size` is between one and
    /// [`SHARED_TOOL_PAGE_MAX_SIZE`], and [`Error::NotFound`] when
    /// the caller cannot inspect the Workspace.
    pub async fn list(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        after: Option<&SharedToolPath>,
        page_size: u32,
    ) -> Result<SharedToolPage, Error> {
        if !(1..=SHARED_TOOL_PAGE_MAX_SIZE).contains(&page_size) {
            return Err(Error::InvalidInput(
                "Shared Tool page size is out of range".to_string(),
            ));
        }
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.authorize_inspection(&mut transaction, actor, workspace_id)
            .await?;
        let page = shared_tool_repository::list_live_page(
            &mut transaction,
            workspace_id,
            after,
            page_size,
        )
        .await?;
        transaction.commit().await?;
        Ok(page)
    }

    /// Lists every revision of the Shared Tool `id`, newest first, to a
    /// Member User. Its identity names it whether it is live or removed: a
    /// removal keeps its revisions.
    ///
    /// # Errors
    ///
    /// Returns [`Error::PermissionDenied`] to an Agent, and
    /// [`Error::NotFound`] when the Workspace has no such Shared Tool or the
    /// caller cannot inspect the Workspace.
    pub async fn list_revisions(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        id: &SharedToolId,
    ) -> Result<Vec<SharedToolRevision>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.authorize_history(&mut transaction, actor, workspace_id)
            .await?;
        let revisions =
            shared_tool_repository::list_revisions(&mut transaction, workspace_id, id).await?;
        transaction.commit().await?;
        // Every Shared Tool has its first revision.
        if revisions.is_empty() {
            return Err(Error::NotFound(AuthoredToolKind::Shared));
        }
        Ok(revisions)
    }

    /// Returns one revision of the Shared Tool `id`, live or removed, to a
    /// Member User.
    ///
    /// # Errors
    ///
    /// Returns [`Error::PermissionDenied`] to an Agent, and
    /// [`Error::NotFound`] when the Workspace has no such Shared Tool, it
    /// has no such revision, or the caller cannot inspect the Workspace.
    pub async fn get_revision(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        id: &SharedToolId,
        revision: u64,
    ) -> Result<SharedToolRevision, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.authorize_history(&mut transaction, actor, workspace_id)
            .await?;
        let found =
            shared_tool_repository::find_revision(&mut transaction, workspace_id, id, revision)
                .await?;
        transaction.commit().await?;
        found.ok_or(Error::NotFound(AuthoredToolKind::Shared))
    }

    /// Lists one page of the lifecycle Events of the Workspace's Shared
    /// Tools, newest first after the Event `after`, to a Member User: every
    /// publish and removal, of removed Shared Tools too. With `shared_tool`,
    /// lists the Events of that Shared Tool alone.
    ///
    /// # Errors
    ///
    /// Returns [`Error::PermissionDenied`] to an Agent,
    /// [`Error::InvalidInput`] unless `page_size` is between one and
    /// [`SHARED_TOOL_PAGE_MAX_SIZE`] and `after` names an Event of the
    /// Workspace, and [`Error::NotFound`] when the caller cannot inspect the
    /// Workspace.
    pub async fn list_lifecycle_events(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        shared_tool: Option<&SharedToolId>,
        after: Option<&SharedToolLifecycleEventId>,
        page_size: u32,
    ) -> Result<SharedToolLifecycleEventPage, Error> {
        if !(1..=SHARED_TOOL_PAGE_MAX_SIZE).contains(&page_size) {
            return Err(Error::InvalidInput(
                "Shared Tool Event page size is out of range".to_string(),
            ));
        }
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.authorize_history(&mut transaction, actor, workspace_id)
            .await?;
        let page = shared_tool_repository::list_lifecycle_event_page(
            &mut transaction,
            workspace_id,
            shared_tool,
            after,
            page_size,
        )
        .await?;
        transaction.commit().await?;
        page.ok_or_else(|| {
            Error::InvalidInput("no Shared Tool Event is at this page position".to_string())
        })
    }

    /// The publish's review and compiled source, from read snapshots: the
    /// write transaction decides again under its holds.
    async fn prepare(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        publish: &SharedToolPublish,
        tools: Option<&dyn ToolDeclarationReader>,
    ) -> Result<PreparedPublish, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let evidence = self
            .workspaces
            .check_in_transaction(
                &mut transaction,
                context.actor(),
                workspace_id,
                WorkspaceAction::Inspect,
            )
            .await?;
        require_owner(&evidence, PUBLISH_DENIED)?;
        read_origin(&mut transaction, context, workspace_id, publish).await?;
        let previous = self
            .current_at_path(&mut transaction, workspace_id, publish)
            .await?;
        transaction.commit().await?;

        let compiled = self
            .authoring
            .prepare(
                workspace_id,
                &AuthoredToolPath::Shared(publish.path.clone()),
                publish.source.as_str(),
                Typing {
                    private_tools_of: None,
                    tools,
                },
            )
            .await?;
        let shared_tools = self
            .dependencies(workspace_id, publish.path(), &compiled)
            .await?;
        let review = SharedToolReview {
            previous,
            declaration: compiled.declaration.clone(),
            diagnostics: compiled.diagnostics.clone(),
            diagnostics_truncated: compiled.diagnostics_truncated,
            called_tools: compiled.called_tools(),
            shared_tools,
            runtime_built_paths: compiled.tools.runtime_built_paths(),
        };
        Ok(PreparedPublish { review, compiled })
    }

    /// The current revision at the publish's path, which the publish must
    /// expect, from a snapshot.
    async fn current_at_path(
        &self,
        transaction: &mut Transaction<'_>,
        workspace_id: &WorkspaceId,
        publish: &SharedToolPublish,
    ) -> Result<Option<SharedToolRevision>, Error> {
        let record =
            shared_tool_repository::find_live_record(transaction, workspace_id, &publish.path)
                .await?;
        require_admitted(transaction, workspace_id, publish, record.as_ref()).await?;
        let Some(record) = record else {
            return Ok(None);
        };
        shared_tool_repository::find_revision(
            transaction,
            workspace_id,
            &record.id,
            record.current_revision,
        )
        .await?
        .ok_or_else(|| Error::CorruptData("Shared Tool lost its current revision".to_string()))
        .map(Some)
    }

    /// The live Shared Tools the compiled source calls, other than the one
    /// it publishes, at their current revisions.
    async fn dependencies(
        &self,
        workspace_id: &WorkspaceId,
        own_path: &SharedToolPath,
        compiled: &PreparedAuthoredTool,
    ) -> Result<Vec<SharedToolDependency>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let dependencies =
            current_dependencies(&mut transaction, workspace_id, own_path, compiled).await?;
        transaction.commit().await?;
        Ok(dependencies)
    }

    async fn publish_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        publish: &SharedToolPublish,
        compiled: PreparedAuthoredTool,
    ) -> Result<SharedTool, Error> {
        self.hold_owner_authority(transaction, context, workspace_id, PUBLISH_DENIED)
            .await?;
        let origin = read_origin(transaction, context, workspace_id, publish).await?;
        // The Workspace hold acquired above serializes the path and
        // namespace decisions, for an absent path too.
        let record =
            shared_tool_repository::hold_live_record(transaction, workspace_id, &publish.path)
                .await?;
        require_admitted(transaction, workspace_id, publish, record.as_ref()).await?;
        // The revisions current in this transaction are the ones recorded.
        // A publish since the review may have moved one: every Run links the
        // current revision of each Shared Tool, so none is a pin.
        let dependencies =
            current_dependencies(transaction, workspace_id, &publish.path, &compiled).await?;
        for dependency in &dependencies {
            if !shared_tool_repository::hold_current(
                transaction,
                workspace_id,
                &dependency.id.tool_key(),
                &dependency.path,
                dependency.revision,
            )
            .await?
            {
                return Err(Error::CorruptData(
                    "a Shared Tool moved inside the transaction that read it".to_string(),
                ));
            }
        }

        let tool = next_revision(
            context,
            workspace_id,
            publish,
            record.as_ref(),
            compiled,
            origin,
        )?;
        let revision = tool.current.revision;
        match &record {
            None => shared_tool_repository::insert_record(transaction, &tool).await?,
            Some(record) => {
                shared_tool_repository::advance_current(
                    transaction,
                    workspace_id,
                    &record.id,
                    record.current_revision,
                    revision,
                )
                .await?;
            }
        }
        shared_tool_repository::insert_revision(transaction, workspace_id, &tool.current).await?;
        shared_tool_repository::insert_dependencies(
            transaction,
            workspace_id,
            &tool.current,
            &dependencies,
        )
        .await?;
        shared_tool_repository::insert_lifecycle_event(
            transaction,
            shared_tool_repository::NewLifecycleEvent {
                workspace_id,
                actor: context.actor().user_id(),
                action: SharedToolLifecycleAction::Published,
                target_id: &tool.id,
                target_path: &tool.path,
                revision,
                content_hash: tool.current.content_hash(),
                occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
                trace_id: context.trace_id(),
            },
        )
        .await?;
        Ok(tool)
    }

    /// Stages the removal of [`Self::remove`] and its lifecycle Event in
    /// the caller's transaction, and returns the removed Shared Tool's
    /// identity. The caller removes its exact rule in the same transaction,
    /// through `coral_policy::remove_authored_tool_rules_in_transaction`,
    /// and sends the identity to `SearchIndex::sync_authored_tool` after the
    /// commit.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`Self::remove`]. Failure requires rollback.
    pub async fn remove_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        path: &SharedToolPath,
        expected_revision: u64,
    ) -> Result<SharedToolId, Error> {
        self.hold_owner_authority(transaction, context, workspace_id, REMOVAL_DENIED)
            .await?;
        let Some(record) =
            shared_tool_repository::hold_live_record(transaction, workspace_id, path).await?
        else {
            return Err(Error::NotFound(AuthoredToolKind::Shared));
        };
        if record.current_revision != expected_revision {
            return Err(Error::RevisionConflict {
                kind: AuthoredToolKind::Shared,
                current: record.current_revision,
            });
        }
        // The Workspace hold keeps a publish from adding a caller before
        // this removal commits.
        if let Some(dependent) =
            shared_tool_repository::first_dependent(transaction, workspace_id, &record.id).await?
        {
            return Err(Error::SharedToolHasDependents { dependent });
        }
        let current = shared_tool_repository::find_revision(
            transaction,
            workspace_id,
            &record.id,
            record.current_revision,
        )
        .await?
        .ok_or_else(|| Error::CorruptData("Shared Tool lost its current revision".to_string()))?;
        shared_tool_repository::tombstone(
            transaction,
            workspace_id,
            &record.id,
            record.current_revision,
            context.occurred_at_unix_nanos(),
        )
        .await?;
        shared_tool_repository::insert_lifecycle_event(
            transaction,
            shared_tool_repository::NewLifecycleEvent {
                workspace_id,
                actor: context.actor().user_id(),
                action: SharedToolLifecycleAction::Removed,
                target_id: &record.id,
                target_path: &record.path,
                revision: record.current_revision,
                content_hash: current.content_hash(),
                occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
                trace_id: context.trace_id(),
            },
        )
        .await?;
        Ok(record.id)
    }

    /// Holds the Workspace and the actor's authority over its Shared Tools
    /// for the write that follows: only a Workspace Owner by Workspace
    /// Membership publishes or removes. The Workspace hold serializes every
    /// publish and removal in the Workspace. `denial` says what a Member
    /// cannot do.
    async fn hold_owner_authority(
        &self,
        transaction: &mut Transaction<'_>,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        denial: &'static str,
    ) -> Result<(), Error> {
        let evidence = self
            .workspaces
            .check_configuration_in_transaction(transaction, context.actor(), workspace_id)
            .await
            .map_err(|error| match error {
                coral_workspace::Error::PermissionDenied(_) => Error::PermissionDenied(denial),
                other => other.into(),
            })?;
        require_owner(&evidence, denial)
    }

    /// Checks that `actor` can read the history of the Workspace's Shared
    /// Tools, which is every revision and lifecycle Event of the live and
    /// the removed ones: every current User Member can, and no Agent.
    async fn authorize_history(
        &self,
        transaction: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
    ) -> Result<(), Error> {
        // The Workspace first, so an Agent of another one learns nothing.
        self.authorize_inspection(transaction, actor, workspace_id)
            .await?;
        if actor.authenticated_user().is_none() {
            return Err(Error::PermissionDenied(
                "Agents cannot read Shared Tool history",
            ));
        }
        Ok(())
    }

    /// Checks that `actor` can read the Workspace's Shared Tools: every
    /// current User Member and Agent can. A denial is concealed as absence.
    async fn authorize_inspection(
        &self,
        transaction: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
    ) -> Result<(), Error> {
        self.workspaces
            .check_principal_inspection_in_snapshot(
                transaction,
                actor,
                workspace_id,
                now_unix_nanos()?,
            )
            .await
            .map(drop)
            .map_err(|error| match error {
                coral_workspace::Error::WorkspaceNotFound
                | coral_workspace::Error::PermissionDenied(_) => {
                    Error::NotFound(AuthoredToolKind::Shared)
                }
                other => other.into(),
            })
    }
}

/// The Shared Tool as the publish leaves it: created at revision 1, or the
/// live `record` at its next revision, with the exact content of the
/// request as its current revision.
fn next_revision(
    context: &ChangeContext,
    workspace_id: &WorkspaceId,
    publish: &SharedToolPublish,
    record: Option<&shared_tool_repository::SharedToolRecord>,
    compiled: PreparedAuthoredTool,
    origin: SharedToolRevisionOrigin,
) -> Result<SharedTool, Error> {
    let occurred_at = context.occurred_at_unix_nanos();
    let (id, revision, created_by, created_at) = match record {
        None => (
            SharedToolId::generate(),
            1,
            context.actor().user_id().clone(),
            occurred_at,
        ),
        Some(record) => (
            record.id.clone(),
            record
                .current_revision
                .checked_add(1)
                .ok_or_else(|| Error::CorruptData("Shared Tool revision overflowed".to_string()))?,
            record.created_by.clone(),
            record.created_at_unix_nanos,
        ),
    };
    Ok(SharedTool {
        workspace_id: workspace_id.clone(),
        path: publish.path.clone(),
        created_by,
        created_at_unix_nanos: created_at,
        current: SharedToolRevision {
            shared_tool_id: id.clone(),
            revision,
            content_hash: content_hash(&publish.description, &publish.source),
            description: publish.description.clone(),
            source: publish.source.clone(),
            declaration: compiled.declaration,
            declaration_field_projection: compiled.declaration_field_projection,
            diagnostics: compiled.diagnostics,
            diagnostics_truncated: compiled.diagnostics_truncated,
            default_access_state: publish.default_access_state,
            published_by: context.actor().user_id().clone(),
            origin,
            published_at_unix_nanos: occurred_at,
        },
        id,
    })
}

/// A publish's review and its compiled source.
struct PreparedPublish {
    review: SharedToolReview,
    compiled: PreparedAuthoredTool,
}

const PUBLISH_DENIED: &str = "only a Workspace Owner can publish a Shared Tool";
const REMOVAL_DENIED: &str = "only a Workspace Owner can remove a Shared Tool";

/// Only a Workspace Owner by Workspace Membership publishes or removes.
fn require_owner(evidence: &WorkspaceEvidence, denial: &'static str) -> Result<(), Error> {
    if evidence.effective_role() == WorkspaceRole::Owner
        && evidence.authority_source() == AuthoritySource::WorkspaceMembership
    {
        Ok(())
    } else {
        Err(Error::PermissionDenied(denial))
    }
}

/// Reads the source a publish copies: the actor's own or an Agent's
/// current Private Tool, or an earlier revision of the Shared Tool at the
/// publish's path. Another User's Private Tool stays unreadable, as it is
/// to every other surface.
async fn read_origin(
    transaction: &mut Transaction<'_>,
    context: &ChangeContext,
    workspace_id: &WorkspaceId,
    publish: &SharedToolPublish,
) -> Result<SharedToolRevisionOrigin, Error> {
    match &publish.origin {
        SharedToolOrigin::PrivateTool { owner, path } => {
            let readable = match owner.kind() {
                PrincipalKind::Agent => true,
                PrincipalKind::User => owner.id().as_str() == context.actor().user_id().as_str(),
            };
            if !readable {
                return Err(Error::SharedToolSourceUnreadable);
            }
            let private_tool =
                repository::find_current_in_transaction(transaction, workspace_id, owner, path)
                    .await?
                    .ok_or(Error::SharedToolSourceUnreadable)?;
            Ok(SharedToolRevisionOrigin::PrivateTool(
                private_tool.id().clone(),
            ))
        }
        SharedToolOrigin::Revision(revision) => {
            let record =
                shared_tool_repository::find_live_record(transaction, workspace_id, &publish.path)
                    .await?
                    .ok_or(Error::SharedToolSourceUnreadable)?;
            let restored = shared_tool_repository::find_revision(
                transaction,
                workspace_id,
                &record.id,
                *revision,
            )
            .await?
            .ok_or(Error::SharedToolSourceUnreadable)?;
            // A restore publishes that revision's content again, so the
            // revision it names is the content the Owner reviewed.
            if restored.source != publish.source || restored.description != publish.description {
                return Err(Error::SharedToolRestoreDiffers);
            }
            Ok(SharedToolRevisionOrigin::Revision(*revision))
        }
    }
}

/// Refuses a publish that does not expect the revision of `record`, the
/// live Shared Tool at its path, or whose new path names a namespace of
/// other live Shared Tools or lies under one of them.
async fn require_admitted(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    publish: &SharedToolPublish,
    record: Option<&shared_tool_repository::SharedToolRecord>,
) -> Result<(), Error> {
    publish.admits(record.map_or(0, |record| record.current_revision))?;
    if record.is_some() {
        return Ok(());
    }
    let live = shared_tool_repository::live_paths(transaction, workspace_id).await?;
    if live
        .iter()
        .any(|other| tool_paths_overlap(other.as_str(), publish.path.as_str()))
    {
        return Err(Error::PathConflict(AuthoredToolKind::Shared));
    }
    Ok(())
}

/// The live Shared Tools `compiled` names, other than `own_path`, at their
/// current revisions. A name that no live Shared Tool has fails as any path
/// that names no Tool.
async fn current_dependencies(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    own_path: &SharedToolPath,
    compiled: &PreparedAuthoredTool,
) -> Result<Vec<SharedToolDependency>, Error> {
    let own = AuthoredToolPath::Shared(own_path.clone());
    let mut dependencies = Vec::new();
    for reference in &compiled.linked {
        let AuthoredToolPath::Shared(path) = reference.path() else {
            continue;
        };
        if path == own_path {
            continue;
        }
        let Some(selection) =
            shared_tool_repository::select_current_source(transaction, workspace_id, path).await?
        else {
            return Err(missing_linked_tool(&own, reference));
        };
        dependencies.push(SharedToolDependency {
            id: selection.id,
            path: path.clone(),
            revision: selection.revision,
        });
    }
    Ok(dependencies)
}

/// The lowercase hex SHA-256 of one revision's content: the description's
/// UTF-8 length as eight big-endian bytes, the description, then the source.
fn content_hash(description: &AuthoredToolDescription, source: &AuthoredToolSource) -> String {
    let description = description.as_str().as_bytes();
    let mut hasher = Sha256::new();
    hasher.update(
        u64::try_from(description.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    hasher.update(description);
    hasher.update(source.as_str().as_bytes());
    let mut encoded = String::with_capacity(64);
    for byte in hasher.finalize() {
        for nibble in [byte >> 4, byte & 0x0f] {
            if let Some(character) = char::from_digit(u32::from(nibble), 16) {
                encoded.push(character);
            }
        }
    }
    encoded
}

#[cfg(test)]
mod tests;
