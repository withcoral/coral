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
//! publishes immutable revisions from the exact content they reviewed and
//! removes a Shared Tool, every Principal in the Workspace reads the live
//! ones, and Member Users read their history.
//!
//! [`PrivateToolInspection`] reads the caller's own current definitions, and
//! an Agent's for a Workspace Owner, without a compiler, for read-only
//! surfaces. [`PrivateToolDeletion`] stages their deletion in the caller's
//! transaction without a compiler, for management surfaces.
//!
//! The discovery facets serve Universal Search for both kinds: they join the
//! caller's authorized read snapshot and separate metadata, declaration
//! ranking, hydration, and exact description material, and they read every
//! Tool of a kind for the search index. A Principal discovers its own Private
//! Tools and the Workspace's live Shared Tools.
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
mod discovery;
mod discovery_repository;
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
pub use discovery::{
    AuthoredToolAuthoringMaterial, AuthoredToolEntry, AuthoredToolIndexRevision,
    AuthoredToolSearchMaterial, ExactAuthoredToolAuthoringMaterial,
    ExactAuthoredToolDescriptionMaterial, authored_tool_entries_in_transaction,
    authored_tool_entry_in_transaction, authored_tool_index_revisions_in_transaction,
    authored_tool_search_material_in_transaction, exact_authored_tool_description_in_transaction,
    hydrate_authored_tool_search_material_in_transaction,
};
pub use error::Error;
pub use inspection::PrivateToolInspection;
pub use linking::{
    InvalidLinkedToolReference, LinkedToolReference, LinkedToolSelection,
    hold_linked_tools_in_transaction, select_linked_tool_in_transaction,
};
pub use private_tool::{
    PreparedSave, PrivateToolModule, delete_current_for_access_removal_in_transaction,
};
pub use shared_tool::{
    SHARED_TOOL_PAGE_MAX_SIZE, SharedTool, SharedToolDefault, SharedToolDependency, SharedToolId,
    SharedToolLifecycleAction, SharedToolLifecycleEvent, SharedToolLifecycleEventId,
    SharedToolLifecycleEventPage, SharedToolModule, SharedToolOrigin, SharedToolPage,
    SharedToolPath, SharedToolPublication, SharedToolPublish, SharedToolReview, SharedToolRevision,
    SharedToolRevisionOrigin, SharedToolSummary,
};
pub use tool_typing::{ToolDeclaration, ToolDeclarationReader};
pub use types::{
    AUTHORED_TOOL_DECLARATION_MAX_BYTES, AUTHORED_TOOL_DESCRIPTION_MAX_BYTES,
    AUTHORED_TOOL_DIAGNOSTIC_MAX_COUNT, AUTHORED_TOOL_DIAGNOSTIC_MESSAGE_MAX_BYTES,
    AUTHORED_TOOL_PATH_MAX_BYTES, AUTHORED_TOOL_PATH_MAX_SEGMENTS, AUTHORED_TOOL_SOURCE_MAX_BYTES,
    AdministrativeLifecycleEvent, AuthoredToolDeclaration, AuthoredToolDeclarationField,
    AuthoredToolDeclarationFieldProjection, AuthoredToolDescription, AuthoredToolDiagnostic,
    AuthoredToolDiagnosticCategory, AuthoredToolId, AuthoredToolPath, AuthoredToolSource,
    COMPILATION_TIMEOUT, LifecycleAction, LifecycleOutcome, PRIVATE_TOOL_PAGE_MAX_SIZE,
    PrivateTool, PrivateToolChangeContext, PrivateToolId, PrivateToolPage, PrivateToolPath,
    PrivateToolSave, PrivateToolSummary,
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

/// The default Tool Access State of each of `tools` that names a live
/// Shared Tool of the Workspace: its current revision's. A key that names a
/// removed Shared Tool is left out like any key that names no Shared Tool.
/// Every Principal in the Workspace reads its live Shared Tools.
///
/// The caller owns Workspace authority and the coherent read transaction.
///
/// # Errors
///
/// Returns a persistence or corrupt-data failure.
pub async fn shared_tool_defaults_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    tools: &[coral_catalog::ToolKey],
) -> Result<std::collections::BTreeMap<coral_catalog::ToolKey, coral_catalog::ToolAccessState>, Error>
{
    Ok(
        shared_tool_repository::live_defaults(transaction, workspace_id, tools)
            .await?
            .into_iter()
            .map(|tool| (tool.key, tool.default_access_state))
            .collect(),
    )
}

/// One page of the Workspace's live Shared Tools with their defaults, in
/// Tool Key order after `after`, with one row more than `page_size` when
/// another page follows.
///
/// The caller owns Workspace authority and the coherent read transaction.
///
/// # Errors
///
/// Returns a persistence or corrupt-data failure.
pub async fn shared_tool_default_page_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    after: Option<&coral_catalog::ToolKey>,
    page_size: u32,
) -> Result<Vec<SharedToolDefault>, Error> {
    shared_tool_repository::live_default_page(transaction, workspace_id, after, page_size).await
}

#[cfg(test)]
mod tests;
