//! The approval an eligible Tool needs, for discovery to describe.
//! Policy owns the exact access decision; a Tool it disables
//! ([`ExactToolRules::disables`]) is neither listed, ranked, described,
//! nor counted toward readiness.

use coral_catalog::{HttpMethod, ToolHints, ToolKey};
use coral_policy::{ExactToolRules, ToolAccessState};

use crate::{ApprovalRequirement, ProviderId};

/// The approval an eligible Tool needs, or `None` when policy disables it.
pub(super) fn tool_approval(
    rules: &ExactToolRules,
    provider_id: &ProviderId,
    tool_key: &ToolKey,
    hints: ToolHints,
    http_method: Option<HttpMethod>,
) -> Option<ApprovalRequirement> {
    match rules.access_state(provider_id, tool_key, hints, http_method) {
        ToolAccessState::Disabled => None,
        ToolAccessState::Enabled => Some(ApprovalRequirement::NotRequired),
        ToolAccessState::ApprovalRequired => Some(ApprovalRequirement::Required),
    }
}
