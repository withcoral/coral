//! The Operation eligibility rule that discovery and focused readiness share.
//! Policy owns the exact access decision; an Operation it disables is neither
//! listed, ranked, described, nor counted toward readiness.

use coral_catalog::{OperationHints, OperationKey};
use coral_policy::{ExactOperationRules, OperationAccessState};

use crate::{ApprovalRequirement, ProviderId};

/// The approval an eligible Operation needs, or `None` when policy disables it.
pub(super) fn operation_approval(
    rules: &ExactOperationRules,
    provider_id: &ProviderId,
    operation_key: &OperationKey,
    hints: OperationHints,
) -> Option<ApprovalRequirement> {
    match rules.access_state(provider_id, operation_key, hints) {
        OperationAccessState::Disabled => None,
        OperationAccessState::Enabled => Some(ApprovalRequirement::NotRequired),
        OperationAccessState::ApprovalRequired => Some(ApprovalRequirement::Required),
    }
}
