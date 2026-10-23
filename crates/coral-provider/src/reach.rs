//! Which Operations a Principal reaches, before any credential check.
//! Discovery, Invocation resolution and management reads all apply this one
//! rule, so "cannot see", "cannot discover" and "cannot call" never disagree.

use coral_catalog::{BuiltinOperation, OperationKey, ProviderGenerationId};
use coral_db::Transaction;
use coral_workspace::{WorkspaceAction, WorkspaceRole};

use crate::{ActiveProvider, Error, ProviderCallRoot};

/// Whether a Principal whose Workspace role is `role` (`None` for an Agent)
/// reaches the Operation `operation` under `call_root`.
///
/// Every Principal reaches every Operation of an Owner-added Provider. A
/// Built-in Operation declares the Workspace action it needs, and the role
/// must permit that action. An Operation that another Coral version defines
/// needs Owner authority, so a rollback shows a non-Owner nothing new.
#[must_use]
pub fn reaches(
    call_root: &ProviderCallRoot,
    operation: &OperationKey,
    role: Option<WorkspaceRole>,
) -> bool {
    match call_root {
        ProviderCallRoot::Owner(_) => true,
        ProviderCallRoot::Builtin => BuiltinOperation::from_key(operation.as_str())
            .map_or(WorkspaceAction::Configure, BuiltinOperation::needs)
            .permitted_to(role),
    }
}

/// The Operations of `provider`'s active Generation that `role` does not
/// reach, for a ranking to leave out. Reads the catalog only for the Built-in
/// Provider.
///
/// # Errors
///
/// Returns a persistence or corrupt-catalog failure.
pub async fn unreached_operations_in_transaction(
    transaction: &mut Transaction<'_>,
    provider: &ActiveProvider,
    role: Option<WorkspaceRole>,
) -> Result<Vec<OperationKey>, Error> {
    let call_root = provider.provider().call_root();
    if matches!(call_root, ProviderCallRoot::Owner(_)) {
        return Ok(Vec::new());
    }
    Ok(coral_catalog::available_search_targets_in_transaction(
        transaction,
        provider.generation_id(),
    )
    .await?
    .into_iter()
    .map(|target| target.key().clone())
    .filter(|key| !reaches(call_root, key, role))
    .collect())
}

/// Whether `role` reaches any Operation of the Provider at `call_root`, whose
/// active Generation is `generation`. A Provider with no Operation the
/// Principal reaches is absent to it. Reads the catalog only for the Built-in
/// Provider.
pub(crate) async fn reaches_provider_in_transaction(
    transaction: &mut Transaction<'_>,
    call_root: &ProviderCallRoot,
    generation: &ProviderGenerationId,
    role: Option<WorkspaceRole>,
) -> Result<bool, Error> {
    if matches!(call_root, ProviderCallRoot::Owner(_)) {
        return Ok(true);
    }
    Ok(
        coral_catalog::available_search_targets_in_transaction(transaction, generation)
            .await?
            .iter()
            .any(|target| reaches(call_root, target.key(), role)),
    )
}
