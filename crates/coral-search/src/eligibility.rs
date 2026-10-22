//! The approval an eligible Operation needs, for discovery to describe.
//! Policy owns the exact access decision; an Operation it disables
//! ([`ExactOperationRules::disables`]) is neither listed, ranked, described,
//! nor counted toward readiness.

use coral_catalog::{HttpMethod, OperationHints, OperationKey};
use coral_policy::{ExactOperationRules, OperationAccessState};

use crate::{ApprovalRequirement, ProviderId};

/// The approval an eligible Operation needs, or `None` when policy disables it.
pub(super) fn operation_approval(
    rules: &ExactOperationRules,
    provider_id: &ProviderId,
    operation_key: &OperationKey,
    hints: OperationHints,
    http_method: Option<HttpMethod>,
) -> Option<ApprovalRequirement> {
    match rules.access_state(provider_id, operation_key, hints, http_method) {
        OperationAccessState::Disabled => None,
        OperationAccessState::Enabled => Some(ApprovalRequirement::NotRequired),
        OperationAccessState::ApprovalRequired => Some(ApprovalRequirement::Required),
    }
}
