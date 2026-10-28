//! Current Saved Function authoring and discovery material for Coral.
//!
//! [`SavedFunctionModule`] owns current definitions, validation, visibility, and
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
//! [`SavedFunctionInspection`] reads the caller's own current definitions
//! without a compiler, for read-only surfaces. [`SavedFunctionDeletion`]
//! deletes them without a compiler, for management surfaces.
//!
//! Search facets join the caller's authorized read snapshot and separate metadata,
//! declaration ranking, hydration, and exact editing material. Admission facets
//! select current source and hold that selection at final acceptance; Run owns
//! closure orchestration, compilation, and persistence of accepted snapshots.
//! These facets do not authorize callers or commit transactions independently.
//!
//! Errors distinguish input, concealment, authority, compilation, and persistence.

mod deletion;
mod error;
mod inspection;
mod repository;
mod saved_function;
#[cfg(any(test, feature = "test-fixtures"))]
pub mod test_fixtures_repository;
mod tool_typing;
mod types;

use coral_db::Transaction;
use coral_workspace::{Principal, WorkspaceId};

pub use coral_v8::IsolateLimits;
pub use deletion::SavedFunctionDeletion;
pub use error::Error;
pub use inspection::SavedFunctionInspection;
pub use repository::{hold_source_selection_in_transaction, select_source_in_transaction};
pub use saved_function::{
    PreparedSave, SavedFunctionModule, delete_current_for_access_removal_in_transaction,
};
pub use tool_typing::{ToolDeclaration, ToolDeclarationReader};
pub use types::SavedFunctionSelection;
pub use types::{
    AdministrativeLifecycleEvent, ExactSavedFunctionAuthoringMaterial,
    ExactSavedFunctionDescriptionMaterial, LifecycleAction, LifecycleOutcome,
    SAVED_FUNCTION_DECLARATION_MAX_BYTES, SAVED_FUNCTION_DESCRIPTION_MAX_BYTES,
    SAVED_FUNCTION_DIAGNOSTIC_MAX_COUNT, SAVED_FUNCTION_DIAGNOSTIC_MESSAGE_MAX_BYTES,
    SAVED_FUNCTION_PAGE_MAX_SIZE, SAVED_FUNCTION_PATH_MAX_BYTES, SAVED_FUNCTION_PATH_MAX_SEGMENTS,
    SAVED_FUNCTION_SOURCE_MAX_BYTES, SavedFunction, SavedFunctionAuthoringMaterial,
    SavedFunctionChangeContext, SavedFunctionDeclaration, SavedFunctionDeclarationField,
    SavedFunctionDeclarationFieldProjection, SavedFunctionDeclarationSearchMaterial,
    SavedFunctionDescription, SavedFunctionDiagnostic, SavedFunctionDiagnosticCategory,
    SavedFunctionEntry, SavedFunctionId, SavedFunctionPage, SavedFunctionPath, SavedFunctionSave,
    SavedFunctionSearchMaterial, SavedFunctionSource, SavedFunctionSummary,
};

/// Loads source-free metadata without consulting declaration relevance material.
///
/// # Errors
///
/// Returns a shared persistence or corrupt target failure.
pub async fn saved_function_search_material_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
) -> Result<Vec<SavedFunctionSearchMaterial>, Error> {
    repository::list_search_material_in_transaction(transaction, workspace_id, principal).await
}

/// Loads one Principal-owned Saved Function's declaration fields, for the
/// evidence of one search hit.
///
/// # Errors
///
/// Returns a shared persistence failure. Projection corruption stays isolated
/// as `None` fields on the function.
pub async fn saved_function_declaration_search_material_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &SavedFunctionPath,
) -> Result<Option<SavedFunctionDeclarationSearchMaterial>, Error> {
    repository::declaration_search_material_in_transaction(
        transaction,
        workspace_id,
        principal,
        path,
    )
    .await
}

/// Reads every current Saved Function of one Workspace, of every Principal, as
/// search entries.
///
/// The caller decides visibility; this facet reads no source.
///
/// # Errors
///
/// Returns a shared persistence or corrupt-definition failure. An unreadable
/// declaration-field projection stays isolated as `None` on its one function.
pub async fn saved_function_entries_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Vec<SavedFunctionEntry>, Error> {
    repository::list_workspace_entries_in_transaction(transaction, workspace_id).await
}

/// Reads every current Saved Function of every Workspace as search entries,
/// for the search index's reconcile task.
///
/// # Errors
///
/// Returns a shared persistence or corrupt-definition failure. An unreadable
/// declaration-field projection stays isolated as `None` on its one function.
pub async fn all_saved_function_entries_in_transaction(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<SavedFunctionEntry>, Error> {
    repository::list_every_entry_in_transaction(transaction).await
}

/// Hydrates one current ranked Saved Function without reading its source.
///
/// # Errors
///
/// Returns a persistence or authoritative-description corruption failure.
pub async fn hydrate_saved_function_search_material_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    target: &SavedFunctionSearchMaterial,
) -> Result<Option<SavedFunctionAuthoringMaterial>, Error> {
    repository::hydrate_search_material_in_transaction(transaction, workspace_id, principal, target)
        .await
}

/// Loads one complete exact Principal-owned Saved Function description.
///
/// The authoritative declaration, source, and diagnostics are read by exact
/// path without selecting or decoding declaration relevance material.
///
/// # Errors
///
/// Returns a persistence or authoritative-description corruption failure.
pub async fn exact_saved_function_description_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &SavedFunctionPath,
) -> Result<Option<ExactSavedFunctionDescriptionMaterial>, Error> {
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
