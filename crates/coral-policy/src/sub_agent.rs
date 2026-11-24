//! The rule on `coral.agent`, which is a Workspace Owner's consent to send
//! the Workspace's data to the Organization's Sub-agent model.
//!
//! The Tool is off until an Owner turns it on through
//! [`ToolPolicyModule::turn_on_sub_agent`](crate::ToolPolicyModule::turn_on_sub_agent),
//! which names the model the Owner saw. A change of the model turns every rule
//! off in its own transaction, so a rule that is on is a consent to the model
//! that is set. A Workspace with no rule counts as off, whatever its catalog
//! declares.

use coral_catalog::{BuiltinTool, ToolKey};
use coral_db::{RowLock, Transaction, UnixNanos};
use coral_provider::ProviderId;
use coral_workspace::WorkspaceId;

use crate::{Error, ToolAccessState, ToolPolicyTarget, provider_read_error, repository};

fn agent_key() -> ToolKey {
    ToolKey::parse(BuiltinTool::Agent.key()).expect("a Built-in Tool's key is a Tool Key")
}

/// `coral.agent` of the Workspace's Built-in Provider.
pub(crate) async fn target_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Option<ToolPolicyTarget>, Error> {
    let builtin = coral_provider::builtin_provider_in_transaction(transaction, workspace_id)
        .await
        .map_err(provider_read_error)?;
    Ok(builtin.map(|provider| target(workspace_id, &provider)))
}

fn target(workspace_id: &WorkspaceId, builtin: &ProviderId) -> ToolPolicyTarget {
    ToolPolicyTarget::new(workspace_id.clone(), builtin.clone(), agent_key())
}

/// Refuses a change that turns `coral.agent` on. One that is on already was
/// consented to, so a change from enabled to approval or back is not refused.
pub(crate) async fn refuse_turn_on_in_transaction(
    transaction: &mut Transaction<'_>,
    changed: &ToolPolicyTarget,
    access_state: ToolAccessState,
) -> Result<(), Error> {
    if access_state == ToolAccessState::Disabled || *changed.tool_key() != agent_key() {
        return Ok(());
    }
    if target_in_transaction(transaction, changed.workspace_id())
        .await?
        .as_ref()
        != Some(changed)
    {
        return Ok(());
    }
    if turned_on(transaction, changed, RowLock::None).await? {
        Ok(())
    } else {
        Err(Error::SubAgentConsentRequired)
    }
}

async fn turned_on(
    transaction: &mut Transaction<'_>,
    target: &ToolPolicyTarget,
    lock: RowLock,
) -> Result<bool, Error> {
    let rule = repository::access_state_in_transaction(transaction, target, lock).await?;
    Ok(matches!(
        rule,
        Some(ToolAccessState::Enabled | ToolAccessState::ApprovalRequired)
    ))
}

/// Whether a Workspace Owner turned `coral.agent` on, enabled or with
/// approval. A Workspace with no rule counts as off.
///
/// # Errors
///
/// Returns a persistence or corrupt-data failure.
pub async fn sub_agent_is_on_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<bool, Error> {
    match target_in_transaction(transaction, workspace_id).await? {
        Some(target) => turned_on(transaction, &target, RowLock::None).await,
        None => Ok(false),
    }
}

/// Turns `coral.agent` off in the Workspace whose Built-in Provider is
/// `builtin`, unless it has a rule. The caller adds the Tool to the Workspace
/// in this transaction. It records no policy Event: the Provider's own update
/// is the record.
///
/// # Errors
///
/// Returns a persistence failure.
pub async fn start_sub_agent_off_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    builtin: &ProviderId,
    occurred_at: UnixNanos,
) -> Result<(), Error> {
    let target = target(workspace_id, builtin);
    let off = ToolAccessState::Disabled;
    repository::insert_access_state_if_absent(transaction, &target, off, occurred_at).await?;
    Ok(())
}

/// Turns `coral.agent` off in every Workspace that has a rule on it. The
/// caller changes the Sub-agent's model in this transaction, under an
/// Organization Owner's authority. It only turns the Tool off, and records no
/// policy Event: the change of the model is the record.
///
/// # Errors
///
/// Returns a persistence or corrupt-data failure.
pub async fn turn_sub_agent_off_in_transaction(
    transaction: &mut Transaction<'_>,
    occurred_at: UnixNanos,
) -> Result<(), Error> {
    let builtin = coral_provider::builtin_providers_in_transaction(transaction)
        .await
        .map_err(provider_read_error)?;
    let providers: Vec<_> = builtin
        .into_iter()
        .map(|(_workspace, provider)| provider)
        .collect();
    repository::disable_for_providers(transaction, &providers, &agent_key(), occurred_at).await
}
