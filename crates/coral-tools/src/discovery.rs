//! What Universal Search reads of Authored Tools: each Principal's current
//! Private Tools and the Workspace's live Shared Tools, through one set of
//! facets and one set of types.
//!
//! A facet that answers a caller takes the Principal and the kind, or a path
//! that names the kind. `coral.private` is always that Principal's own, and
//! `coral.shared` is always the Workspace's, as linking resolves them (ADR
//! 0034). A facet that feeds the index reads every Tool of both kinds and
//! leaves visibility to its caller. No facet authorizes a caller, reads a removed
//! Shared Tool, or commits a transaction.

use coral_catalog::{AuthoredToolKind, ToolAccessState};
use coral_db::Transaction;
use coral_workspace::{Principal, WorkspaceId};

use crate::types::{
    AuthoredToolDeclaration, AuthoredToolDeclarationField, AuthoredToolDescription,
    AuthoredToolDiagnostic, AuthoredToolId, AuthoredToolPath, AuthoredToolSource,
};
use crate::{Error, discovery_repository};

/// Minimal owner evidence for comparing an Authored Tool's search
/// projection; contains no source, declaration, description, or caller
/// visibility decision.
#[derive(Debug, Clone)]
pub struct AuthoredToolIndexRevision {
    pub(crate) id: AuthoredToolId,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) revision: u64,
}

impl AuthoredToolIndexRevision {
    /// The Tool's stable identity.
    #[must_use]
    pub const fn id(&self) -> &AuthoredToolId {
        &self.id
    }

    /// Its owning Workspace.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    /// A Private Tool's definition revision, or a Shared Tool's current
    /// revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// What Universal Search indexes about one current Authored Tool. Never the
/// source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoredToolEntry {
    pub(crate) id: AuthoredToolId,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) principal: Option<Principal>,
    pub(crate) path: AuthoredToolPath,
    pub(crate) description: AuthoredToolDescription,
    pub(crate) declaration_fields: Option<Vec<AuthoredToolDeclarationField>>,
    pub(crate) revision: u64,
}

impl AuthoredToolEntry {
    /// Returns the stable identity, which names the kind. A Private Tool
    /// keeps it across replacements at its path, and a Shared Tool across
    /// its revisions.
    #[must_use]
    pub const fn id(&self) -> &AuthoredToolId {
        &self.id
    }

    /// Returns the Workspace that owns the Tool.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    /// Returns the Principal who owns a Private Tool, the only one who can
    /// see it. `None` for a Shared Tool, which every Principal of its
    /// Workspace can see.
    #[must_use]
    pub const fn principal(&self) -> Option<&Principal> {
        self.principal.as_ref()
    }

    /// Returns the path below the kind's Call Root.
    #[must_use]
    pub const fn path(&self) -> &AuthoredToolPath {
        &self.path
    }

    /// Returns the exact explicit description.
    #[must_use]
    pub const fn description(&self) -> &AuthoredToolDescription {
        &self.description
    }

    /// Returns the readable declaration paths with their text, or `None`
    /// when none was projected or the stored projection is unreadable. The
    /// Tool is still searchable by path and description.
    #[must_use]
    pub fn declaration_fields(&self) -> Option<&[AuthoredToolDeclarationField]> {
        self.declaration_fields.as_deref()
    }

    /// Returns a Private Tool's definition revision, or a Shared Tool's
    /// current revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// Metadata-only target of one Authored Tool a Principal can discover,
/// supplied before listing or ranking.
///
/// This projection contains no source, declaration, relevance material,
/// diagnostics, revision, or accepted executable material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoredToolSearchMaterial {
    pub(crate) id: AuthoredToolId,
    pub(crate) path: AuthoredToolPath,
    pub(crate) description: AuthoredToolDescription,
    pub(crate) default_access_state: ToolAccessState,
}

impl AuthoredToolSearchMaterial {
    /// Returns the Tool Access State that applies when no exact rule
    /// matches the Tool: a Private Tool's author declared it, and a Shared
    /// Tool's publishing Owner chose it.
    #[must_use]
    pub const fn default_access_state(&self) -> ToolAccessState {
        self.default_access_state
    }

    /// Returns the Tool's stable identity.
    #[must_use]
    pub const fn id(&self) -> &AuthoredToolId {
        &self.id
    }

    /// Returns the path below the kind's Call Root.
    #[must_use]
    pub const fn path(&self) -> &AuthoredToolPath {
        &self.path
    }

    /// Returns the exact explicit description.
    #[must_use]
    pub const fn description(&self) -> &AuthoredToolDescription {
        &self.description
    }
}

/// Current material of one Authored Tool supplied to ordinary Universal
/// Search.
///
/// This projection contains only secret-free authoring contract fields. It
/// carries no source, diagnostics, identity, revision, author, Membership,
/// Run, or accepted executable evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoredToolAuthoringMaterial {
    pub(crate) path: AuthoredToolPath,
    pub(crate) description: AuthoredToolDescription,
    pub(crate) declaration: AuthoredToolDeclaration,
}

impl AuthoredToolAuthoringMaterial {
    /// Returns the path below the kind's Call Root.
    #[must_use]
    pub const fn path(&self) -> &AuthoredToolPath {
        &self.path
    }

    /// Returns the exact explicit authoring description.
    #[must_use]
    pub const fn description(&self) -> &AuthoredToolDescription {
        &self.description
    }

    /// Returns the exact self-contained compiler-emitted declaration.
    #[must_use]
    pub const fn declaration(&self) -> &AuthoredToolDeclaration {
        &self.declaration
    }
}

/// The source of one Authored Tool and the diagnostics stored with it,
/// supplied only to exact description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactAuthoredToolAuthoringMaterial {
    pub(crate) source: AuthoredToolSource,
    pub(crate) diagnostics: Vec<AuthoredToolDiagnostic>,
    pub(crate) diagnostics_truncated: bool,
}

impl ExactAuthoredToolAuthoringMaterial {
    /// Returns the exact current source for authorized exact description.
    #[must_use]
    pub const fn source(&self) -> &AuthoredToolSource {
        &self.source
    }

    /// Returns bounded advisory authoring diagnostics.
    #[must_use]
    pub fn diagnostics(&self) -> &[AuthoredToolDiagnostic] {
        &self.diagnostics
    }

    /// Returns whether additional diagnostics were omitted by the authoring bound.
    #[must_use]
    pub const fn diagnostics_truncated(&self) -> bool {
        self.diagnostics_truncated
    }
}

/// Complete relevance-independent exact description material of one
/// Authored Tool. It never names who saved or published the Tool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactAuthoredToolDescriptionMaterial {
    pub(crate) id: AuthoredToolId,
    pub(crate) authoring: AuthoredToolAuthoringMaterial,
    pub(crate) detail: ExactAuthoredToolAuthoringMaterial,
    pub(crate) revision: u64,
    pub(crate) default_access_state: ToolAccessState,
}

impl ExactAuthoredToolDescriptionMaterial {
    /// Returns the Tool's stable identity, its Tool Key.
    #[must_use]
    pub const fn id(&self) -> &AuthoredToolId {
        &self.id
    }

    /// Returns a Private Tool's definition revision, or a Shared Tool's
    /// current revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the Tool Access State that applies when no exact rule
    /// matches the Tool.
    #[must_use]
    pub const fn default_access_state(&self) -> ToolAccessState {
        self.default_access_state
    }

    /// Returns source-free public authoring material.
    #[must_use]
    pub const fn authoring(&self) -> &AuthoredToolAuthoringMaterial {
        &self.authoring
    }

    /// Returns the source and the diagnostics stored with it.
    #[must_use]
    pub const fn detail(&self) -> &ExactAuthoredToolAuthoringMaterial {
        &self.detail
    }
}

/// Loads the source-free metadata of every Authored Tool of `kind` that
/// `principal` can discover, in path order: its own Private Tools, or the
/// Workspace's live Shared Tools. Reads no declaration relevance material.
///
/// # Errors
///
/// Returns a shared persistence or corrupt target failure.
pub async fn authored_tool_search_material_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    kind: AuthoredToolKind,
) -> Result<Vec<AuthoredToolSearchMaterial>, Error> {
    discovery_repository::list_search_material(transaction, workspace_id, principal, kind).await
}

/// Hydrates one current ranked Authored Tool without reading its source.
/// `None` when `principal` can no longer discover `target`.
///
/// # Errors
///
/// Returns a persistence or authoritative-description corruption failure.
pub async fn hydrate_authored_tool_search_material_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    target: &AuthoredToolSearchMaterial,
) -> Result<Option<AuthoredToolAuthoringMaterial>, Error> {
    discovery_repository::hydrate_search_material(transaction, workspace_id, principal, target)
        .await
}

/// Loads the complete exact description of the Authored Tool at `path`
/// that `principal` can discover: its own Private Tool, or the Workspace's
/// live Shared Tool at its current revision.
///
/// The authoritative declaration, source, and diagnostics are read by exact
/// path without selecting or decoding declaration relevance material. The
/// material names no author: a Shared Tool's publisher is attribution only.
///
/// # Errors
///
/// Returns a persistence or authoritative-description corruption failure.
pub async fn exact_authored_tool_description_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &AuthoredToolPath,
) -> Result<Option<ExactAuthoredToolDescriptionMaterial>, Error> {
    discovery_repository::exact_description(transaction, workspace_id, principal, path).await
}

/// Reads every current Authored Tool in every Workspace as search entries,
/// for the search index's reconcile task: every Principal's Private Tools,
/// then every live Shared Tool.
///
/// The caller decides visibility; this facet reads no source.
///
/// # Errors
///
/// Returns a shared persistence or corrupt-definition failure. An unreadable
/// declaration-field projection stays isolated as `None` on its one Tool.
pub async fn authored_tool_entries_in_transaction(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<AuthoredToolEntry>, Error> {
    let mut entries = Vec::new();
    for kind in AuthoredToolKind::ALL {
        entries.extend(discovery_repository::entries(transaction, kind).await?);
    }
    Ok(entries)
}

/// Reads one current Authored Tool as a search entry, `None` when the
/// Workspace holds no such Private Tool or live Shared Tool.
///
/// The caller decides visibility; this facet reads no source.
///
/// # Errors
///
/// Returns a shared persistence or corrupt-definition failure. An unreadable
/// declaration-field projection stays isolated as `None` on the Tool.
pub async fn authored_tool_entry_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &AuthoredToolId,
) -> Result<Option<AuthoredToolEntry>, Error> {
    discovery_repository::entry(transaction, workspace_id, id).await
}

/// Reads only the identities and current revisions of every current
/// Authored Tool, every Private Tool and every live Shared Tool, for the
/// background index's telemetry snapshot and for finding search rows
/// without a Tool. The caller decides visibility; no declaration is read.
///
/// # Errors
///
/// Returns a persistence or corrupt revision failure.
pub async fn authored_tool_index_revisions_in_transaction(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<AuthoredToolIndexRevision>, Error> {
    let mut revisions = Vec::new();
    for kind in AuthoredToolKind::ALL {
        revisions.extend(discovery_repository::index_revisions(transaction, kind).await?);
    }
    Ok(revisions)
}
