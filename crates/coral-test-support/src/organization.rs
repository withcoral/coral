//! Explicit User fixtures through the same Organization provisioning seam as login.
use coral_db::Database;
use coral_workspace::{
    AuthenticatedUser, ChangeContext, OrganizationModule, OrganizationRole, TraceId, UserId,
};
use std::sync::Arc;

/// Provisions initial test Membership and returns its current authority snapshot.
/// Existing roles and disablement are never overwritten by repeated fixture setup.
///
/// # Panics
/// Panics when the fixture identity is invalid or provisioning fails.
pub async fn provision_user(
    database: Arc<Database>,
    id: &str,
    initial_role: OrganizationRole,
) -> AuthenticatedUser {
    let user_id = UserId::parse(id).expect("valid fixture User id");
    let initial = AuthenticatedUser::organization_member(user_id.clone(), initial_role);
    let membership = OrganizationModule::new(database)
        .provision_login(&ChangeContext::new(initial, 1, TraceId::current_or_new()))
        .await
        .expect("provision test Organization Membership");
    if membership.enabled() {
        AuthenticatedUser::organization_member(user_id, membership.role())
    } else {
        AuthenticatedUser::without_organization_membership(user_id)
    }
}
