//! The exact rule of a Private or a Shared Tool goes with the Tool. The rule
//! rows are coral-policy's, which depends on coral-tools, so a workflow here
//! owns the removal's transaction and joins both.
use coral_tools::{AuthoredToolId, Error};
use coral_workspace::WorkspaceId;

/// Removes the exact rule of `removed`, the Private or Shared Tool the
/// caller removed in `transaction`. The facet fails only on persistence or
/// corrupt data, which the removal's own error reports as it does its own.
pub(crate) async fn remove_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
    removed: impl Into<AuthoredToolId>,
) -> Result<(), Error> {
    let removed = removed.into();
    coral_policy::remove_authored_tool_rules_in_transaction(
        transaction,
        workspace_id,
        removed.kind(),
        &[removed.tool_key()],
    )
    .await
    .map_err(|error| match error {
        coral_policy::Error::Database(error) => Error::Database(error),
        other => Error::CorruptData(other.to_string()),
    })
}
