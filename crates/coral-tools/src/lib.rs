//! The Tools that Coral performs from authored source: each Principal's current
//! Private Tools and the Workspace's Shared Tools, their authoring and their
//! discovery material. The two kinds share one authoring seam: the same
//! compiler prepares either source, and the same types hold its description,
//! source, declaration and diagnostics.
//!
//! [`PrivateToolModule`] owns current definitions, validation, visibility, and
//! atomic Administrative Lifecycle Events. It uses canonical Workspace authority
//! and Coral's pinned compiler-only V8 adapter, never Run execution.
//!
//! Save compiles outside its write transaction, then holds current authority and
//! literal dependencies before replacing source, description, declaration, and
//! bounded diagnostics together. Failure changes neither definition nor evidence.
//! Occurrence timestamps are metadata, not concurrency controls. Membership
//! removal discards current definitions where Workspace access ends, with
//! explicit deletion evidence in Administration's transaction.
//!
//! [`SharedToolModule`] owns the Workspace's Shared Tools: a Workspace Owner
//! publishes immutable revisions from the exact content they reviewed, and
//! every Principal in the Workspace reads them.
//!
//! [`PrivateToolInspection`] reads the caller's own current definitions
//! without a compiler, for read-only surfaces. [`PrivateToolDeletion`]
//! deletes them without a compiler, for management surfaces.
//!
//! Search facets join the caller's authorized read snapshot and separate metadata,
//! declaration ranking, hydration, and exact editing material.
//!
//! The linking facets serve Program admission for both kinds: they select
//! the current definition of each Private or Shared Tool a source names, as
//! one [`LinkedToolSelection`], and hold every selection at final
//! acceptance. They own the order and the strength of those holds. Run owns
//! closure orchestration, compilation, and persistence of accepted
//! snapshots. No facet authorizes callers or commits a transaction.
//!
//! Errors distinguish input, concealment, authority, compilation, and
//! persistence, and name the kind of Tool where both kinds can fail the same
//! way.

mod authoring;
mod deletion;
mod error;
mod inspection;
mod linking;
mod private_tool;
mod repository;
mod shared_tool;
mod shared_tool_repository;
#[cfg(any(test, feature = "test-fixtures"))]
pub mod test_fixtures_repository;
mod tool_typing;
mod types;
pub mod usage_repository;

use coral_db::Transaction;
use coral_workspace::{Principal, WorkspaceId};

pub use coral_v8::IsolateLimits;
pub use deletion::PrivateToolDeletion;
pub use error::Error;
pub use inspection::PrivateToolInspection;
pub use linking::{
    InvalidLinkedToolReference, LinkedToolPath, LinkedToolReference, LinkedToolSelection,
    hold_linked_tools_in_transaction, select_linked_tool_in_transaction,
};
pub use private_tool::{
    PreparedSave, PrivateToolModule, delete_current_for_access_removal_in_transaction,
};
pub use shared_tool::{
    SHARED_TOOL_PAGE_MAX_SIZE, SharedTool, SharedToolDependency, SharedToolId, SharedToolModule,
    SharedToolOrigin, SharedToolPage, SharedToolPath, SharedToolPublication, SharedToolPublish,
    SharedToolReview, SharedToolRevision, SharedToolRevisionOrigin, SharedToolSummary,
};
pub use tool_typing::{ToolDeclaration, ToolDeclarationReader};
pub use types::{
    AUTHORED_TOOL_DECLARATION_MAX_BYTES, AUTHORED_TOOL_DESCRIPTION_MAX_BYTES,
    AUTHORED_TOOL_DIAGNOSTIC_MAX_COUNT, AUTHORED_TOOL_DIAGNOSTIC_MESSAGE_MAX_BYTES,
    AUTHORED_TOOL_PATH_MAX_BYTES, AUTHORED_TOOL_PATH_MAX_SEGMENTS, AUTHORED_TOOL_SOURCE_MAX_BYTES,
    AdministrativeLifecycleEvent, AuthoredToolDeclaration, AuthoredToolDeclarationField,
    AuthoredToolDeclarationFieldProjection, AuthoredToolDescription, AuthoredToolDiagnostic,
    AuthoredToolDiagnosticCategory, AuthoredToolSource, COMPILATION_TIMEOUT,
    ExactPrivateToolAuthoringMaterial, ExactPrivateToolDescriptionMaterial, LifecycleAction,
    LifecycleOutcome, PRIVATE_TOOL_PAGE_MAX_SIZE, PrivateTool, PrivateToolAuthoringMaterial,
    PrivateToolChangeContext, PrivateToolDeclarationSearchMaterial, PrivateToolEntry,
    PrivateToolId, PrivateToolIndexRevision, PrivateToolPage, PrivateToolPath, PrivateToolSave,
    PrivateToolSearchMaterial, PrivateToolSummary,
};
pub use types::{PrivateToolScope, ReadablePrivateTool};

/// The declared default Tool Access State of each of `tools` that names a
/// current Private Tool of the Workspace within `scope` of `reader`. No
/// scope holds another User's Private Tool, so its key is left out like any
/// key that names no Private Tool.
///
/// The caller owns Workspace authority and the coherent read transaction.
///
/// # Errors
///
/// Returns a persistence or corrupt-data failure.
pub async fn readable_defaults_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    reader: &Principal,
    scope: PrivateToolScope,
    tools: &[coral_catalog::ToolKey],
) -> Result<std::collections::BTreeMap<coral_catalog::ToolKey, coral_catalog::ToolAccessState>, Error>
{
    repository::readable_defaults_in_transaction(transaction, workspace_id, reader, scope, tools)
        .await
}

/// One page of the current Private Tools within `scope` of `reader`, in
/// Tool Key order after `after`, with one row more than `page_size` when
/// another page follows.
///
/// The caller owns Workspace authority and the coherent read transaction.
///
/// # Errors
///
/// Returns a persistence or corrupt-data failure.
pub async fn readable_page_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    reader: &Principal,
    scope: PrivateToolScope,
    after: Option<&coral_catalog::ToolKey>,
    page_size: u32,
) -> Result<Vec<ReadablePrivateTool>, Error> {
    repository::readable_page_in_transaction(
        transaction,
        workspace_id,
        reader,
        scope,
        after,
        page_size,
    )
    .await
}

/// Loads source-free metadata without consulting declaration relevance material.
///
/// # Errors
///
/// Returns a shared persistence or corrupt target failure.
pub async fn private_tool_search_material_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
) -> Result<Vec<PrivateToolSearchMaterial>, Error> {
    repository::list_search_material_in_transaction(transaction, workspace_id, principal).await
}

/// Loads one Principal-owned Private Tool's declaration fields, for the
/// evidence of one search hit.
///
/// # Errors
///
/// Returns a shared persistence failure. Projection corruption stays isolated
/// as `None` fields on the Private Tool.
pub async fn private_tool_declaration_search_material_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &PrivateToolPath,
) -> Result<Option<PrivateToolDeclarationSearchMaterial>, Error> {
    repository::declaration_search_material_in_transaction(
        transaction,
        workspace_id,
        principal,
        path,
    )
    .await
}

/// Reads every current Private Tool of one Workspace, of every Principal, as
/// search entries.
///
/// The caller decides visibility; this facet reads no source.
///
/// # Errors
///
/// Returns a shared persistence or corrupt-definition failure. An unreadable
/// declaration-field projection stays isolated as `None` on its one Private Tool.
pub async fn private_tool_entries_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Vec<PrivateToolEntry>, Error> {
    repository::list_workspace_entries_in_transaction(transaction, workspace_id).await
}

/// Reads every current Private Tool of every Workspace as search entries,
/// for the search index's reconcile task.
///
/// # Errors
///
/// Returns a shared persistence or corrupt-definition failure. An unreadable
/// declaration-field projection stays isolated as `None` on its one Private Tool.
pub async fn all_private_tool_entries_in_transaction(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<PrivateToolEntry>, Error> {
    repository::list_every_entry_in_transaction(transaction).await
}

/// Reads one current Private Tool as a search entry, `None` when the
/// Workspace holds no such Private Tool.
///
/// The caller decides visibility; this facet reads no source.
///
/// # Errors
///
/// Returns a shared persistence or corrupt-definition failure. An unreadable
/// declaration-field projection stays isolated as `None` on the Private Tool.
pub async fn private_tool_entry_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &PrivateToolId,
) -> Result<Option<PrivateToolEntry>, Error> {
    repository::entry_in_transaction(transaction, workspace_id, id).await
}

/// Reads only identities and current revisions for the background index's
/// telemetry snapshot and for finding search rows without a Private Tool. The
/// caller decides visibility; no declaration is read.
///
/// # Errors
/// Returns a persistence or corrupt revision failure.
pub async fn private_tool_index_revisions_in_transaction(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<PrivateToolIndexRevision>, Error> {
    repository::index_revisions(transaction).await
}

/// Hydrates one current ranked Private Tool without reading its source.
///
/// # Errors
///
/// Returns a persistence or authoritative-description corruption failure.
pub async fn hydrate_private_tool_search_material_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    target: &PrivateToolSearchMaterial,
) -> Result<Option<PrivateToolAuthoringMaterial>, Error> {
    repository::hydrate_search_material_in_transaction(transaction, workspace_id, principal, target)
        .await
}

/// Loads one complete exact Principal-owned Private Tool description.
///
/// The authoritative declaration, source, and diagnostics are read by exact
/// path without selecting or decoding declaration relevance material.
///
/// # Errors
///
/// Returns a persistence or authoritative-description corruption failure.
pub async fn exact_private_tool_description_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &PrivateToolPath,
) -> Result<Option<ExactPrivateToolDescriptionMaterial>, Error> {
    repository::exact_description_material_in_transaction(
        transaction,
        workspace_id,
        principal,
        path,
    )
    .await
}

#[cfg(test)]
mod tests;
