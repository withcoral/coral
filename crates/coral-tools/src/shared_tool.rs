//! Shared Tools: the Workspace-owned Tools at `coral.shared` that a
//! Workspace Owner publishes (ADR 0033).
//!
//! [`SharedToolModule`] owns publishing and the reads of every Principal in
//! the Workspace. A publish compiles the reviewed source outside its write
//! transaction, then holds Owner authority, the source it copies and the
//! current revision at its path, and writes one immutable revision, its
//! dependencies and one lifecycle Event together. Its author is attribution
//! only: no access removal reads or writes these rows.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use coral_catalog::ToolAccessState;
use coral_db::{Database, RowLock, Transaction};
use coral_run_executor::CompilerClient;
use coral_v8::{CallableDeclaration, CoralDeclarations};
use coral_workspace::{
    AuthenticatedPrincipal, AuthoritySource, ChangeContext, Principal, PrincipalKind, UserId,
    WorkspaceAction, WorkspaceEvidence, WorkspaceId, WorkspaceModule, WorkspaceRole,
};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::private_tool::{
    AuthoredKind, PreparedPrivateTool, PrivateToolCompiler, now_unix_nanos,
    prepare_authored_source, private_tool_paths_overlap, v8_compiler,
};
use crate::types::{
    PRIVATE_TOOL_DECLARATION_MAX_BYTES, PrivateToolDeclaration,
    PrivateToolDeclarationFieldProjection, PrivateToolDescription, PrivateToolDiagnostic,
    PrivateToolId, PrivateToolPath, PrivateToolSource,
};
use crate::{Error, repository, shared_tool_repository};

/// The most Shared Tools one page lists.
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
/// live Shared Tool in its Workspace. It follows the rules of a Private Tool
/// Path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SharedToolPath(PrivateToolPath);

impl SharedToolPath {
    /// Validates the fixed path bounds and safe JavaScript segments.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for a path a Private Tool could not
    /// have.
    pub fn parse(value: &str) -> Result<Self, Error> {
        PrivateToolPath::parse(value).map(Self).map_err(|_error| {
            Error::InvalidInput(format!(
                "Shared Tool Path must contain 1 through {} non-reserved safe JavaScript identifier segments and be at most {} UTF-8 bytes",
                crate::PRIVATE_TOOL_PATH_MAX_SEGMENTS,
                crate::PRIVATE_TOOL_PATH_MAX_BYTES,
            ))
        })
    }

    /// Returns the complete dotted path relative to `coral.shared`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Returns validated segments for the compiler's linked source input.
    #[must_use]
    pub fn segments(&self) -> Vec<String> {
        self.0.segments()
    }

    pub(crate) const fn as_private_tool_path(&self) -> &PrivateToolPath {
        &self.0
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

/// One Workspace Owner's request to publish the exact content they
/// reviewed as a new revision of the Shared Tool at `path`.
#[derive(Debug, Clone)]
pub struct SharedToolPublish {
    path: SharedToolPath,
    origin: SharedToolOrigin,
    description: PrivateToolDescription,
    source: PrivateToolSource,
    default_access_state: ToolAccessState,
    expected_revision: u64,
    validate_only: bool,
}

impl SharedToolPublish {
    /// Publishes `source` and `description`, the content the Owner
    /// reviewed, from `origin`, at `path`, with `default_access_state` as
    /// its default Tool Access State. `expected_revision` is the revision
    /// the Owner expects to be current at `path`: zero to create.
    #[must_use]
    pub const fn new(
        path: SharedToolPath,
        origin: SharedToolOrigin,
        description: PrivateToolDescription,
        source: PrivateToolSource,
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
            validate_only: false,
        }
    }

    /// Checks the publish and returns its review without writing anything.
    #[must_use]
    pub const fn validate_only(mut self) -> Self {
        self.validate_only = true;
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
            Err(Error::SharedToolRevisionConflict { current })
        }
    }
}

/// One immutable, numbered publication of a Shared Tool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedToolRevision {
    pub(crate) shared_tool_id: SharedToolId,
    pub(crate) revision: u64,
    pub(crate) description: PrivateToolDescription,
    pub(crate) source: PrivateToolSource,
    pub(crate) declaration: PrivateToolDeclaration,
    pub(crate) declaration_field_projection: PrivateToolDeclarationFieldProjection,
    pub(crate) diagnostics: Vec<PrivateToolDiagnostic>,
    pub(crate) diagnostics_truncated: bool,
    pub(crate) content_hash: String,
    pub(crate) default_access_state: ToolAccessState,
    pub(crate) published_by: UserId,
    pub(crate) source_private_tool_id: Option<PrivateToolId>,
    pub(crate) restored_from_revision: Option<u64>,
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
    pub const fn description(&self) -> &PrivateToolDescription {
        &self.description
    }

    /// Returns the exact published source.
    #[must_use]
    pub const fn source(&self) -> &PrivateToolSource {
        &self.source
    }

    /// Returns the declaration emitted from the source.
    #[must_use]
    pub const fn declaration(&self) -> &PrivateToolDeclaration {
        &self.declaration
    }

    /// Returns the searchable declaration fields, or why they are missing.
    #[must_use]
    pub const fn declaration_field_projection(&self) -> &PrivateToolDeclarationFieldProjection {
        &self.declaration_field_projection
    }

    /// Returns the advisory diagnostics retained with the revision.
    #[must_use]
    pub fn diagnostics(&self) -> &[PrivateToolDiagnostic] {
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

    /// Returns the Private Tool it copied, which may since have changed or
    /// gone; `None` for a restore.
    #[must_use]
    pub const fn source_private_tool_id(&self) -> Option<&PrivateToolId> {
        self.source_private_tool_id.as_ref()
    }

    /// Returns the earlier revision a restore copied; `None` otherwise.
    #[must_use]
    pub const fn restored_from_revision(&self) -> Option<u64> {
        self.restored_from_revision
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
    declaration: PrivateToolDeclaration,
    diagnostics: Vec<PrivateToolDiagnostic>,
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
    pub const fn declaration(&self) -> &PrivateToolDeclaration {
        &self.declaration
    }

    /// Returns the advisory diagnostics of the reviewed source.
    #[must_use]
    pub fn diagnostics(&self) -> &[PrivateToolDiagnostic] {
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
/// the publish only validated.
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
    /// publish that only validated.
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
    pub(crate) description: PrivateToolDescription,
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
    pub const fn description(&self) -> &PrivateToolDescription {
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

/// The current revision of one Shared Tool that a Run links, held through
/// its acceptance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedToolSelection {
    pub(crate) id: SharedToolId,
    pub(crate) path: SharedToolPath,
    pub(crate) revision: u64,
    pub(crate) source: PrivateToolSource,
    pub(crate) default_access_state: ToolAccessState,
}

impl SharedToolSelection {
    /// Returns the Shared Tool's stable identity.
    #[must_use]
    pub const fn id(&self) -> &SharedToolId {
        &self.id
    }

    /// Returns its path relative to `coral.shared`.
    #[must_use]
    pub const fn path(&self) -> &SharedToolPath {
        &self.path
    }

    /// Returns the revision the Run links.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns that revision's source.
    #[must_use]
    pub const fn source(&self) -> &PrivateToolSource {
        &self.source
    }

    /// Returns the default Tool Access State the Owner chose for that
    /// revision, which applies when no exact rule matches.
    #[must_use]
    pub const fn default_access_state(&self) -> ToolAccessState {
        self.default_access_state
    }

    /// The code Execution Binding of the linked revision.
    #[must_use]
    pub fn execution_binding(&self) -> coral_catalog::CodeExecutionBinding {
        coral_catalog::CodeExecutionBinding::new(self.id.to_string(), self.revision)
    }
}

/// Selects the current revision of the live Shared Tool at `path`, for a
/// Run to link. `None` when no live Shared Tool is there.
///
/// The caller owns Workspace authority and the coherent read transaction.
///
/// # Errors
///
/// Returns a persistence or corrupt-data failure.
pub async fn select_shared_source_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    path: &SharedToolPath,
) -> Result<Option<SharedToolSelection>, Error> {
    shared_tool_repository::select_current_source(transaction, workspace_id, path).await
}

/// Holds a selected revision current through the caller's acceptance
/// commit, with a shared row lock, so concurrent acceptances that link the
/// same Shared Tool read side by side while a publish waits for them.
/// `false` means the Shared Tool moved or was removed: admission must
/// reject without a Run.
///
/// # Errors
///
/// Returns a persistence failure.
pub async fn hold_shared_selection_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    selection: &SharedToolSelection,
) -> Result<bool, Error> {
    shared_tool_repository::hold_current(transaction, workspace_id, selection, RowLock::Shared)
        .await
}

/// The Workspace's Shared Tools: Owner publishing, and reads for every
/// Principal in the Workspace.
#[derive(Clone)]
pub struct SharedToolModule {
    database: Arc<Database>,
    workspaces: WorkspaceModule,
    compiler: Arc<dyn PrivateToolCompiler>,
    compilation_timeout: Duration,
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
            database,
            workspaces,
            compiler: v8_compiler(compiler),
            compilation_timeout,
        }
    }

    /// Publishes the exact content the Owner reviewed as the next revision
    /// of the Shared Tool at its path, and makes it current; or, for a
    /// publish that only validates, returns its review and writes nothing.
    ///
    /// The source is compiled outside the write transaction, and never
    /// executed. The write transaction holds the Owner's authority, the
    /// source the publish copies and the current revision at the path, then
    /// writes the revision, the Shared Tools it calls and one lifecycle
    /// Event together. The Private Tool it copies stays as it was.
    ///
    /// This publish does not notify Universal Search.
    ///
    /// # Errors
    ///
    /// - [`Error::PermissionDenied`] unless the actor is a Workspace Owner by
    ///   Workspace Membership.
    /// - [`Error::SharedToolSourceUnreadable`] when the origin is another
    ///   User's Private Tool, a Private Tool that is gone, or a revision that
    ///   never existed.
    /// - [`Error::SharedToolRevisionConflict`] when the current revision at
    ///   the path is not the expected one.
    /// - [`Error::SharedToolPathConflict`] when the path overlaps another
    ///   live Shared Tool's namespace.
    /// - An authoring failure when the source cannot become a Tool.
    ///
    /// Failure changes nothing.
    pub async fn publish(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        publish: SharedToolPublish,
    ) -> Result<SharedToolPublication, Error> {
        let prepared = self.prepare(context, workspace_id, &publish).await?;
        if publish.validate_only {
            return Ok(SharedToolPublication {
                review: prepared.review,
                published: None,
            });
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

    /// Returns the live Shared Tool at `path` with its current revision, to
    /// any Principal in the Workspace.
    ///
    /// # Errors
    ///
    /// Returns [`Error::SharedToolNotFound`] when no live Shared Tool is
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
        tool.ok_or(Error::SharedToolNotFound)
    }

    /// Lists one page of the Workspace's live Shared Tools in path order
    /// after `after`, to any Principal in the Workspace.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] unless `page_size` is between one and
    /// [`SHARED_TOOL_PAGE_MAX_SIZE`], and [`Error::SharedToolNotFound`] when
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

    /// Lists every revision of the live Shared Tool at `path`, newest first,
    /// to any Principal in the Workspace.
    ///
    /// # Errors
    ///
    /// Returns [`Error::SharedToolNotFound`] when no live Shared Tool is
    /// there or the caller cannot inspect the Workspace.
    pub async fn list_revisions(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        path: &SharedToolPath,
    ) -> Result<Vec<SharedToolRevision>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.authorize_inspection(&mut transaction, actor, workspace_id)
            .await?;
        let Some(record) =
            shared_tool_repository::find_live_record(&mut transaction, workspace_id, path, false)
                .await?
        else {
            return Err(Error::SharedToolNotFound);
        };
        let revisions =
            shared_tool_repository::list_revisions(&mut transaction, workspace_id, &record.id)
                .await?;
        transaction.commit().await?;
        Ok(revisions)
    }

    /// Returns one revision of the live Shared Tool at `path`, to any
    /// Principal in the Workspace.
    ///
    /// # Errors
    ///
    /// Returns [`Error::SharedToolNotFound`] when no live Shared Tool is
    /// there, it has no such revision, or the caller cannot inspect the
    /// Workspace.
    pub async fn get_revision(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        path: &SharedToolPath,
        revision: u64,
    ) -> Result<SharedToolRevision, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.authorize_inspection(&mut transaction, actor, workspace_id)
            .await?;
        let Some(record) =
            shared_tool_repository::find_live_record(&mut transaction, workspace_id, path, false)
                .await?
        else {
            return Err(Error::SharedToolNotFound);
        };
        let found = shared_tool_repository::find_revision(
            &mut transaction,
            workspace_id,
            &record.id,
            revision,
        )
        .await?;
        transaction.commit().await?;
        found.ok_or(Error::SharedToolNotFound)
    }

    /// The publish's review and compiled source, from read snapshots: the
    /// write transaction decides again under its holds.
    async fn prepare(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        publish: &SharedToolPublish,
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
        require_publishing_owner(&evidence)?;
        read_origin(&mut transaction, context, workspace_id, publish).await?;
        let previous = self
            .current_at_path(&mut transaction, workspace_id, publish)
            .await?;
        transaction.commit().await?;

        let deadline = Instant::now() + self.compilation_timeout;
        let source = publish.source.as_str();
        let named = {
            let compiler = self.compiler.clone();
            let analyzed = source.to_string();
            tokio::task::spawn_blocking(move || compiler.call_paths(deadline, &analyzed))
                .await
                .map_err(|_join_error| Error::CompilerWorkerStopped)?
        };
        let shared_paths = named
            .as_ref()
            .map(|named| {
                named
                    .shared_tools()
                    .iter()
                    .filter_map(|reference| SharedToolPath::parse(reference.path()).ok())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let declarations = self
            .shared_declarations(workspace_id, publish.path(), &shared_paths)
            .await?;
        let compiled = prepare_authored_source(
            self.compiler.clone(),
            deadline,
            AuthoredKind::Shared,
            publish.path.as_private_tool_path(),
            source,
            declarations,
        )
        .await?;
        let shared_tools = self
            .dependencies(workspace_id, publish.path(), &compiled)
            .await?;
        let mut called_tools = named
            .as_ref()
            .map(|named| named.tools().called_paths())
            .unwrap_or_default();
        called_tools.extend(
            compiled
                .references
                .iter()
                .map(|reference| format!("coral.private.{}", reference.path.as_str())),
        );
        called_tools.extend(
            compiled
                .shared_references
                .iter()
                .map(|reference| format!("coral.shared.{}", reference.path.as_str())),
        );
        called_tools.sort();
        called_tools.dedup();
        let review = SharedToolReview {
            previous,
            declaration: compiled.declaration.clone(),
            diagnostics: compiled.diagnostics.clone(),
            diagnostics_truncated: compiled.diagnostics_truncated,
            called_tools,
            shared_tools,
            runtime_built_paths: named
                .as_ref()
                .map(|named| named.tools().runtime_built_paths())
                .unwrap_or_default(),
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
        let record = shared_tool_repository::find_live_record(
            transaction,
            workspace_id,
            &publish.path,
            false,
        )
        .await?;
        publish.admits(record.as_ref().map_or(0, |record| record.current_revision))?;
        if record.is_none() {
            require_free_namespace(transaction, workspace_id, &publish.path).await?;
        }
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

    /// The current declarations of the live Shared Tools at `paths`, to type
    /// the source with; `None` when it names none.
    async fn shared_declarations(
        &self,
        workspace_id: &WorkspaceId,
        own_path: &SharedToolPath,
        paths: &[SharedToolPath],
    ) -> Result<Option<CoralDeclarations>, Error> {
        if paths.is_empty() {
            return Ok(None);
        }
        let mut transaction = self.database.begin_read_snapshot().await?;
        let mut declarations = Vec::new();
        for path in paths.iter().filter(|path| *path != own_path) {
            if let Some(tool) =
                shared_tool_repository::find_live_tool(&mut transaction, workspace_id, path).await?
            {
                declarations.push(CallableDeclaration::new(
                    path.segments(),
                    tool.current.declaration.as_str().to_string(),
                ));
            }
        }
        transaction.commit().await?;
        Ok(Some(
            CoralDeclarations::new(PRIVATE_TOOL_DECLARATION_MAX_BYTES)
                .with_shared_tools(declarations),
        ))
    }

    /// The live Shared Tools the compiled source calls, other than the one
    /// it publishes, at their current revisions.
    async fn dependencies(
        &self,
        workspace_id: &WorkspaceId,
        own_path: &SharedToolPath,
        compiled: &PreparedPrivateTool,
    ) -> Result<Vec<SharedToolDependency>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let dependencies =
            held_dependencies(&mut transaction, workspace_id, own_path, compiled, false).await?;
        transaction.commit().await?;
        Ok(dependencies)
    }

    async fn publish_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        publish: &SharedToolPublish,
        compiled: PreparedPrivateTool,
    ) -> Result<SharedTool, Error> {
        let evidence = self
            .workspaces
            .check_configuration_in_transaction(transaction, context.actor(), workspace_id)
            .await
            .map_err(|error| match error {
                coral_workspace::Error::PermissionDenied(_) => {
                    Error::PermissionDenied("only a Workspace Owner can publish a Shared Tool")
                }
                other => other.into(),
            })?;
        require_publishing_owner(&evidence)?;
        let origin = read_origin(transaction, context, workspace_id, publish).await?;
        // The Workspace hold acquired above serializes the path and
        // namespace decisions, for an absent path too.
        let record = shared_tool_repository::find_live_record(
            transaction,
            workspace_id,
            &publish.path,
            true,
        )
        .await?;
        publish.admits(record.as_ref().map_or(0, |record| record.current_revision))?;
        if record.is_none() {
            require_free_namespace(transaction, workspace_id, &publish.path).await?;
        }
        let dependencies =
            held_dependencies(transaction, workspace_id, &publish.path, &compiled, true).await?;

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
                | coral_workspace::Error::PermissionDenied(_) => Error::SharedToolNotFound,
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
    compiled: PreparedPrivateTool,
    origin: ReadOrigin,
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
            source_private_tool_id: origin.private_tool,
            restored_from_revision: origin.revision,
            published_at_unix_nanos: occurred_at,
        },
        id,
    })
}

/// A publish's review and its compiled source.
struct PreparedPublish {
    review: SharedToolReview,
    compiled: PreparedPrivateTool,
}

/// What a publish copies, as its revision records it.
struct ReadOrigin {
    private_tool: Option<PrivateToolId>,
    revision: Option<u64>,
}

/// A publish's lifecycle action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SharedToolLifecycleAction {
    Published,
}

impl SharedToolLifecycleAction {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Published => "shared_tool.published",
        }
    }
}

/// Only a Workspace Owner by Workspace Membership publishes.
fn require_publishing_owner(evidence: &WorkspaceEvidence) -> Result<(), Error> {
    if evidence.effective_role() == WorkspaceRole::Owner
        && evidence.authority_source() == AuthoritySource::WorkspaceMembership
    {
        Ok(())
    } else {
        Err(Error::PermissionDenied(
            "only a Workspace Owner can publish a Shared Tool",
        ))
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
) -> Result<ReadOrigin, Error> {
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
            Ok(ReadOrigin {
                private_tool: Some(private_tool.id().clone()),
                revision: None,
            })
        }
        SharedToolOrigin::Revision(revision) => {
            let record = shared_tool_repository::find_live_record(
                transaction,
                workspace_id,
                &publish.path,
                false,
            )
            .await?
            .ok_or(Error::SharedToolSourceUnreadable)?;
            shared_tool_repository::find_revision(transaction, workspace_id, &record.id, *revision)
                .await?
                .ok_or(Error::SharedToolSourceUnreadable)?;
            Ok(ReadOrigin {
                private_tool: None,
                revision: Some(*revision),
            })
        }
    }
}

/// Refuses a new path that names a namespace of other live Shared Tools or
/// lies under one of them.
async fn require_free_namespace(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    path: &SharedToolPath,
) -> Result<(), Error> {
    let live = shared_tool_repository::live_paths(transaction, workspace_id).await?;
    if live.iter().any(|other| {
        private_tool_paths_overlap(other.as_private_tool_path(), path.as_private_tool_path())
    }) {
        return Err(Error::SharedToolPathConflict);
    }
    Ok(())
}

/// The live Shared Tools `compiled` names, other than `own_path`, at their
/// current revisions; held shared through the write when `hold` is set. A
/// name that no live Shared Tool has fails as any path that names no Tool.
async fn held_dependencies(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    own_path: &SharedToolPath,
    compiled: &PreparedPrivateTool,
    hold: bool,
) -> Result<Vec<SharedToolDependency>, Error> {
    let mut dependencies = Vec::new();
    for reference in &compiled.shared_references {
        let path = SharedToolPath(reference.path.clone());
        if &path == own_path {
            continue;
        }
        let Some(selection) =
            shared_tool_repository::select_current_source(transaction, workspace_id, &path).await?
        else {
            return Err(Error::SharedToolAdmission(format!(
                "{}:{}:{}: {}",
                AuthoredKind::Shared.source_name(own_path.as_private_tool_path()),
                reference.line,
                reference.column,
                coral_catalog::unknown_call_path_message(
                    &format!("coral.shared.{}", path.as_str()),
                    &[],
                )
            )));
        };
        if hold
            && !shared_tool_repository::hold_current(
                transaction,
                workspace_id,
                &selection,
                RowLock::Shared,
            )
            .await?
        {
            return Err(Error::CorruptData(
                "a held Shared Tool moved inside its transaction".to_string(),
            ));
        }
        dependencies.push(SharedToolDependency {
            id: selection.id,
            path,
            revision: selection.revision,
        });
    }
    Ok(dependencies)
}

/// The lowercase hex SHA-256 of one revision's content: the description's
/// UTF-8 length as eight big-endian bytes, the description, then the source.
fn content_hash(description: &PrivateToolDescription, source: &PrivateToolSource) -> String {
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
