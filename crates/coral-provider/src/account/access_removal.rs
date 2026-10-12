//! Credential retirement joins the transaction owning access loss.
use super::{AuthorizationState, connection, event_visible_to, finish_authorization, repository};
use crate::{Error, LifecycleAction, LifecycleOutcome};
use coral_db::Transaction;
use coral_workspace::{ChangeContext, PreparedAccessRemoval, PrincipalKind, UserId};

/// Retires current caller credentials and pending authorizations in lost scopes.
/// Shared and Agent credentials survive the departure of their provisioning User.
/// # Errors
/// Rejects foreign transaction evidence; any failure requires full rollback.
pub async fn retire_for_access_removal_in_transaction(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    prepared: &PreparedAccessRemoval,
) -> Result<(), Error> {
    prepared.verify_transaction(tx, context)?;
    let authority = prepared.authority_source();
    for lost in prepared.lost_access() {
        let owner = lost.workspace_id().clone();
        let private_user = match lost.principal().kind() {
            PrincipalKind::User => Some(UserId::parse(lost.principal().id().as_str())?),
            PrincipalKind::Agent => None,
        };
        for id in repository::access_authorizations(tx, lost).await? {
            let mut row = repository::load_authorization(tx, &owner, id).await?;
            finish_authorization(
                tx,
                context,
                &owner,
                authority,
                &mut row,
                AuthorizationState::Canceled,
                None,
            )
            .await?;
        }
        if let Some(user) = &private_user {
            super::setup_oauth::retire_for_access_removal(
                tx,
                context,
                lost.workspace_id(),
                user,
                authority,
            )
            .await?;
            connection::retire_for_access_removal(
                tx,
                context,
                lost.workspace_id(),
                user,
                authority,
            )
            .await?;
        }
        // Keep scrubbed, advanced fences even for initial connections that never
        // completed. Rejoining creates a new current Account; old IDs and versions
        // cannot become replacement targets or accept late token material.
        for account in repository::access_accounts(tx, lost).await? {
            repository::retire_access_account(tx, lost, account, &uuid::Uuid::new_v4().to_string())
                .await?;
            event_visible_to(
                tx,
                context,
                &owner,
                authority,
                LifecycleAction::AccountDeleted,
                private_user.as_ref(),
                &account.to_string(),
                LifecycleOutcome::Accepted,
            )
            .await?;
        }
    }
    Ok(())
}
