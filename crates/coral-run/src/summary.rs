//! Windowed totals of the execution history a caller can see in one Workspace.
use crate::{CoralFailure, Error, InvocationResultKind, RunModule};
use coral_workspace::{HistoryAuthority, Principal};
use std::cmp::Reverse;
use std::collections::BTreeMap;

/// The longest window a summary covers.
pub const MAX_SUMMARY_DAYS: u32 = 30;
const NANOS_PER_DAY: i64 = 86_400 * 1_000_000_000;

/// One window's totals of the caller's own work and their Agents' work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivitySummary {
    /// Invocations that ended in the window, by outcome.
    pub calls: CallOutcomes,
    /// Call Paths with stopped Invocations, most stopped first. Only a resolved
    /// Call Path is stored, so the list holds at most one row per Tool.
    pub stopped: Vec<StoppedTool>,
    /// Call Paths with ended Invocations, busiest first, bounded like `stopped`.
    pub tools: Vec<ToolCalls>,
    /// Approval requests decided in the window.
    pub approvals: ApprovalDecisions,
    /// Net context bytes of the measured work that ended in the window, as Activity
    /// counts it; absent when none was measured.
    pub context_impact_bytes: Option<i64>,
    /// A Run in scope is still running, or a Run that ended in the window has no
    /// measurement, so the total leaves some work out.
    pub context_impact_partial: bool,
    /// Net context bytes per invoking Principal, most saved first.
    pub actors: Vec<ActorContext>,
    /// Ended Invocations per invoking Principal, busiest first. They add up to `calls`.
    pub callers: Vec<CallerCalls>,
}

/// Ended Invocations by who acts next. The six counts add up to every ended Invocation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CallOutcomes {
    /// One complete upstream response, which may still be an error response.
    pub answered: i64,
    /// A Workspace, operator or authority rule stopped the call.
    pub blocked: i64,
    /// A person declined the approval or the Account connection, or let the approval expire.
    pub not_allowed: i64,
    /// A Provider, Account or credential needs repair before the call can run. An
    /// Account connection request that expired unanswered leaves the call unauthorized,
    /// so it counts here.
    pub needs_setup: i64,
    /// The Program's call or the upstream failed, or the result is unknown.
    pub failed: i64,
    /// The Run ended before the call did.
    pub canceled: i64,
}

/// One Call Path's stopped Invocations, by who must act.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoppedTool {
    /// Retained Program Call Path.
    pub call_path: String,
    /// Invocations a rule stopped.
    pub blocked: i64,
    /// Invocations a person did not allow.
    pub not_allowed: i64,
    /// Invocations that need setup.
    pub needs_setup: i64,
}

/// One Call Path's ended Invocations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolCalls {
    /// Retained Program Call Path.
    pub call_path: String,
    /// Ended Invocations.
    pub calls: i64,
    /// The same Invocations per invoking Principal, busiest first.
    pub callers: Vec<CallerCalls>,
}

/// One invoking Principal's ended Invocations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallerCalls {
    /// Accountable invoking Principal.
    pub principal: Principal,
    /// Safe retained Agent label for Agent work; absent for User work or when no label is retained.
    pub agent_label: Option<String>,
    /// Ended Invocations.
    pub calls: i64,
}

/// Human approval decisions in a window. Expired and closed requests are not decisions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ApprovalDecisions {
    /// Approved requests.
    pub approved: i64,
    /// Declined requests.
    pub declined: i64,
    /// Median nanoseconds from request to decision; absent when nobody decided.
    pub median_wait_nanos: Option<i64>,
}

/// One invoking Principal's net context bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActorContext {
    /// Accountable invoking Principal.
    pub principal: Principal,
    /// Safe retained Agent label for Agent work; absent for User work or when no label is retained.
    pub agent_label: Option<String>,
    /// Net bytes; negative means saved.
    pub bytes: i64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CallOutcome {
    Answered,
    Blocked,
    NotAllowed,
    NeedsSetup,
    Failed,
    Canceled,
}

/// Names every failure so a new one needs an explicit summary decision.
const fn outcome(kind: InvocationResultKind, failure: Option<CoralFailure>) -> CallOutcome {
    use CoralFailure as F;
    match (kind, failure) {
        (InvocationResultKind::UpstreamOutcome, _) => CallOutcome::Answered,
        (InvocationResultKind::RunTerminated, _) => CallOutcome::Canceled,
        (
            InvocationResultKind::CoralFailure,
            Some(
                F::ToolDisabled
                | F::ApprovalRequired
                | F::AuthorityLost
                | F::OutboundDestinationRejected,
            ),
        ) => CallOutcome::Blocked,
        (
            InvocationResultKind::CoralFailure,
            Some(F::ApprovalDeclined | F::ApprovalExpired | F::AccountConnectionDeclined),
        ) => CallOutcome::NotAllowed,
        (
            InvocationResultKind::CoralFailure,
            Some(
                F::AccountRequired
                | F::AccountConnectionExpired
                | F::CredentialRefreshFailed
                | F::OAuthClientConfiguration
                | F::AccountAuthorizationChanged
                | F::CredentialRouteChanged
                | F::ProviderUnavailable
                | F::ProviderNamespaceConflict
                | F::ToolUnavailable,
            ),
        ) => CallOutcome::NeedsSetup,
        (
            InvocationResultKind::CoralFailure,
            Some(
                F::ToolNotFound
                | F::ProviderNotFound
                | F::ProviderPublicationFailed
                | F::InvalidInput
                | F::InputTooLarge
                | F::UpstreamIncomplete
                | F::UpstreamRejected
                | F::ResultTooLarge
                | F::RunOutputTooLarge,
            )
            | None,
        )
        | (InvocationResultKind::Indeterminate, _) => CallOutcome::Failed,
    }
}

/// Grouped ended Invocations, as the repository returns them.
pub(crate) struct OutcomeEvidence {
    pub principal: Principal,
    pub agent_label: Option<String>,
    pub call_path: String,
    pub kind: InvocationResultKind,
    pub failure: Option<CoralFailure>,
    pub calls: i64,
}

impl CallOutcomes {
    fn total_mut(&mut self, outcome: CallOutcome) -> &mut i64 {
        match outcome {
            CallOutcome::Answered => &mut self.answered,
            CallOutcome::Blocked => &mut self.blocked,
            CallOutcome::NotAllowed => &mut self.not_allowed,
            CallOutcome::NeedsSetup => &mut self.needs_setup,
            CallOutcome::Failed => &mut self.failed,
            CallOutcome::Canceled => &mut self.canceled,
        }
    }
}

impl StoppedTool {
    /// The count an outcome adds to, or `None` when the call was not stopped.
    fn total_mut(&mut self, outcome: CallOutcome) -> Option<&mut i64> {
        match outcome {
            CallOutcome::Blocked => Some(&mut self.blocked),
            CallOutcome::NotAllowed => Some(&mut self.not_allowed),
            CallOutcome::NeedsSetup => Some(&mut self.needs_setup),
            CallOutcome::Answered | CallOutcome::Failed | CallOutcome::Canceled => None,
        }
    }

    const fn total(&self) -> i64 {
        self.blocked + self.not_allowed + self.needs_setup
    }
}

/// Adds `calls` to `principal`'s entry.
fn add_caller(callers: &mut Vec<CallerCalls>, group: &OutcomeEvidence) {
    match callers.iter_mut().find(|c| c.principal == group.principal) {
        Some(caller) => caller.calls += group.calls,
        None => callers.push(CallerCalls {
            principal: group.principal.clone(),
            agent_label: group.agent_label.clone(),
            calls: group.calls,
        }),
    }
}

/// Busiest first. Principal order breaks ties, so both backends return one order.
fn busiest_first(callers: &mut [CallerCalls]) {
    callers.sort_by(|a, b| {
        (Reverse(a.calls), a.principal.id()).cmp(&(Reverse(b.calls), b.principal.id()))
    });
}

struct Tally {
    calls: CallOutcomes,
    stopped: Vec<StoppedTool>,
    tools: Vec<ToolCalls>,
    callers: Vec<CallerCalls>,
}

fn tally(evidence: Vec<OutcomeEvidence>) -> Tally {
    let mut calls = CallOutcomes::default();
    let mut stopped = BTreeMap::<String, StoppedTool>::new();
    let mut ended = BTreeMap::<String, ToolCalls>::new();
    let mut callers = Vec::new();
    for group in evidence {
        let outcome = outcome(group.kind, group.failure);
        *calls.total_mut(outcome) += group.calls;
        let op = stopped
            .entry(group.call_path.clone())
            .or_insert_with(|| StoppedTool {
                call_path: group.call_path.clone(),
                blocked: 0,
                not_allowed: 0,
                needs_setup: 0,
            });
        if let Some(total) = op.total_mut(outcome) {
            *total += group.calls;
        }
        let tool = ended
            .entry(group.call_path.clone())
            .or_insert_with(|| ToolCalls {
                call_path: group.call_path.clone(),
                calls: 0,
                callers: Vec::new(),
            });
        tool.calls += group.calls;
        add_caller(&mut tool.callers, &group);
        add_caller(&mut callers, &group);
    }
    let mut stopped: Vec<_> = stopped.into_values().filter(|op| op.total() > 0).collect();
    // Stable sorts keep Call Path order among equal counts.
    stopped.sort_by_key(|op| Reverse(op.total()));
    let mut tools: Vec<_> = ended.into_values().collect();
    tools.sort_by_key(|op| Reverse(op.calls));
    for tool in &mut tools {
        busiest_first(&mut tool.callers);
    }
    busiest_first(&mut callers);
    Tally {
        calls,
        stopped,
        tools,
        callers,
    }
}

impl RunModule {
    /// Returns the start of a summary window that ends now and spans `days`.
    /// # Errors
    /// Rejects windows outside 1–30 days.
    pub fn summary_window_start(&self, days: u32) -> Result<i64, Error> {
        if !(1..=MAX_SUMMARY_DAYS).contains(&days) {
            return Err(Error::InvalidInput("invalid summary window".into()));
        }
        Ok(self.now()?.saturating_sub(i64::from(days) * NANOS_PER_DAY))
    }

    /// Sums the Runs in `authority` that ended at or after `since`. Never loads
    /// arguments, results or source. `context` holds the net bytes that work
    /// outside Runs added in the same window; it joins each Principal's Run total.
    /// # Errors
    /// Returns persistence failures.
    pub async fn activity_summary_in_transaction(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        authority: &HistoryAuthority,
        since: i64,
        context: Vec<ActorContext>,
    ) -> Result<ActivitySummary, Error> {
        let evidence = crate::summary_repository::call_outcomes(tx, authority, since).await?;
        let approvals = crate::summary_repository::approval_decisions(tx, authority, since).await?;
        let mut actors = crate::summary_repository::actor_context(tx, authority, since).await?;
        let context_impact_partial =
            crate::summary_repository::unmeasured(tx, authority, since).await?;
        for share in context {
            match actors.iter_mut().find(|a| a.principal == share.principal) {
                Some(actor) => actor.bytes += share.bytes,
                None => actors.push(share),
            }
        }
        // Principal order breaks ties, so both backends return one order.
        actors.sort_by(|a, b| (a.bytes, a.principal.id()).cmp(&(b.bytes, b.principal.id())));
        let Tally {
            calls,
            stopped,
            tools,
            callers,
        } = tally(evidence);
        Ok(ActivitySummary {
            calls,
            stopped,
            tools,
            approvals,
            context_impact_bytes: (!actors.is_empty())
                .then(|| actors.iter().map(|actor| actor.bytes).sum()),
            context_impact_partial,
            actors,
            callers,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcomes_sort_each_end_by_who_acts_next() {
        use CallOutcome as O;
        use CoralFailure as F;
        use InvocationResultKind as K;
        let failure = |code| (K::CoralFailure, Some(code));
        let table = [
            ((K::UpstreamOutcome, None), O::Answered),
            (failure(F::ToolDisabled), O::Blocked),
            (failure(F::ApprovalRequired), O::Blocked),
            (failure(F::AuthorityLost), O::Blocked),
            (failure(F::OutboundDestinationRejected), O::Blocked),
            (failure(F::ApprovalDeclined), O::NotAllowed),
            (failure(F::ApprovalExpired), O::NotAllowed),
            (failure(F::AccountConnectionDeclined), O::NotAllowed),
            (failure(F::AccountConnectionExpired), O::NeedsSetup),
            (failure(F::AccountRequired), O::NeedsSetup),
            (failure(F::CredentialRefreshFailed), O::NeedsSetup),
            (failure(F::OAuthClientConfiguration), O::NeedsSetup),
            (failure(F::AccountAuthorizationChanged), O::NeedsSetup),
            (failure(F::CredentialRouteChanged), O::NeedsSetup),
            (failure(F::ProviderUnavailable), O::NeedsSetup),
            (failure(F::ProviderNamespaceConflict), O::NeedsSetup),
            (failure(F::ToolUnavailable), O::NeedsSetup),
            (failure(F::ToolNotFound), O::Failed),
            (failure(F::ProviderNotFound), O::Failed),
            (failure(F::ProviderPublicationFailed), O::Failed),
            (failure(F::InvalidInput), O::Failed),
            (failure(F::InputTooLarge), O::Failed),
            (failure(F::UpstreamIncomplete), O::Failed),
            (failure(F::UpstreamRejected), O::Failed),
            (failure(F::ResultTooLarge), O::Failed),
            (failure(F::RunOutputTooLarge), O::Failed),
            ((K::CoralFailure, None), O::Failed),
            ((K::Indeterminate, None), O::Failed),
            ((K::RunTerminated, None), O::Canceled),
        ];
        for ((kind, failure), expected) in table {
            assert!(outcome(kind, failure) == expected, "{kind:?} {failure:?}");
        }
        for code in F::ALL {
            assert!(
                table
                    .iter()
                    .any(|((_, failure), _)| *failure == Some(*code)),
                "{code:?} has no expected outcome"
            );
        }
    }
}
