//! Windowed totals of the execution history a caller can see in one Workspace.
use crate::{CoralFailure, Error, InvocationResultKind, RunModule};
use coral_workspace::{AuthenticatedPrincipal, Principal, WorkspaceId};
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
    /// Call Path is stored, so the list holds at most one row per Operation.
    pub stopped: Vec<StoppedOperation>,
    /// Call Paths with ended Invocations, busiest first, bounded like `stopped`.
    pub operations: Vec<OperationCalls>,
    /// Approval requests decided in the window.
    pub approvals: ApprovalDecisions,
    /// Net context bytes of measured Runs that ended in the window; absent when none were measured.
    pub context_impact_bytes: Option<i64>,
    /// A Run in scope is still running, or a Run that ended in the window has no
    /// measurement, so the total leaves some work out.
    pub context_impact_partial: bool,
    /// Net context bytes per invoking Principal, most saved first.
    pub actors: Vec<ActorContext>,
}

/// Ended Invocations by who acts next. The six counts add up to every ended Invocation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CallOutcomes {
    /// One complete upstream response, which may still be an error response.
    pub answered: i64,
    /// A Workspace, operator or authority rule stopped the call.
    pub blocked: i64,
    /// A person declined the approval or the Account connection, or let it expire.
    pub not_allowed: i64,
    /// A Provider, Account or credential needs repair before the call can run.
    pub needs_setup: i64,
    /// The Program's call or the upstream failed, or the result is unknown.
    pub failed: i64,
    /// The Run ended before the call did.
    pub canceled: i64,
}

/// One Call Path's stopped Invocations, by who must act.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoppedOperation {
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
pub struct OperationCalls {
    /// Retained Program Call Path.
    pub call_path: String,
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
    /// Safe retained Agent label, absent for User work or a removed Agent.
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
                F::OperationDisabled
                | F::ApprovalRequired
                | F::AuthorityLost
                | F::OutboundDestinationRejected,
            ),
        ) => CallOutcome::Blocked,
        (
            InvocationResultKind::CoralFailure,
            Some(
                F::ApprovalDeclined
                | F::ApprovalExpired
                | F::AccountConnectionDeclined
                | F::AccountConnectionExpired,
            ),
        ) => CallOutcome::NotAllowed,
        (
            InvocationResultKind::CoralFailure,
            Some(
                F::AccountRequired
                | F::CredentialRefreshFailed
                | F::OAuthClientConfiguration
                | F::AccountAuthorizationChanged
                | F::CredentialRouteChanged
                | F::ProviderUnavailable
                | F::ProviderNamespaceConflict
                | F::ProviderPublicationFailed
                | F::OperationUnavailable,
            ),
        ) => CallOutcome::NeedsSetup,
        (
            InvocationResultKind::CoralFailure,
            Some(
                F::OperationNotFound
                | F::ProviderNotFound
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

impl StoppedOperation {
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

fn tally(
    evidence: Vec<OutcomeEvidence>,
) -> (CallOutcomes, Vec<StoppedOperation>, Vec<OperationCalls>) {
    let mut calls = CallOutcomes::default();
    let mut stopped = BTreeMap::<String, StoppedOperation>::new();
    let mut ended = BTreeMap::<String, i64>::new();
    for group in evidence {
        let outcome = outcome(group.kind, group.failure);
        *calls.total_mut(outcome) += group.calls;
        let op = stopped
            .entry(group.call_path.clone())
            .or_insert_with(|| StoppedOperation {
                call_path: group.call_path.clone(),
                blocked: 0,
                not_allowed: 0,
                needs_setup: 0,
            });
        if let Some(total) = op.total_mut(outcome) {
            *total += group.calls;
        }
        *ended.entry(group.call_path).or_default() += group.calls;
    }
    let mut stopped: Vec<_> = stopped.into_values().filter(|op| op.total() > 0).collect();
    // Stable sorts keep Call Path order among equal counts.
    stopped.sort_by_key(|op| Reverse(op.total()));
    let mut operations: Vec<_> = ended
        .into_iter()
        .map(|(call_path, calls)| OperationCalls { call_path, calls })
        .collect();
    operations.sort_by_key(|op| Reverse(op.calls));
    (calls, stopped, operations)
}

impl RunModule {
    /// Sums the caller's own work and Agent work in owned Workspaces over the last
    /// `days`, in one read snapshot. Never loads arguments, results or source.
    /// # Errors
    /// Rejects windows outside 1–30 days, Agent callers and inaccessible Workspaces.
    pub async fn activity_summary(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        days: u32,
    ) -> Result<ActivitySummary, Error> {
        if !(1..=MAX_SUMMARY_DAYS).contains(&days) {
            return Err(Error::InvalidInput("invalid summary window".into()));
        }
        let now = self.now()?;
        let since = now.saturating_sub(i64::from(days) * NANOS_PER_DAY);
        let mut tx = self.database.begin_read_snapshot().await?;
        let authority = self
            .workspaces
            .action_history_authority_in_transaction(&mut tx, actor, Some(workspace))
            .await?;
        let evidence = crate::summary_repository::call_outcomes(&mut tx, &authority, since).await?;
        let approvals =
            crate::summary_repository::approval_decisions(&mut tx, &authority, since).await?;
        let mut actors =
            crate::summary_repository::actor_context(&mut tx, &authority, since).await?;
        let context_impact_partial =
            crate::summary_repository::unmeasured(&mut tx, &authority, since).await?;
        tx.commit().await?;
        // Principal order breaks ties, so both backends return one order.
        actors.sort_by(|a, b| (a.bytes, a.principal.id()).cmp(&(b.bytes, b.principal.id())));
        let (calls, stopped, operations) = tally(evidence);
        Ok(ActivitySummary {
            calls,
            stopped,
            operations,
            approvals,
            context_impact_bytes: (!actors.is_empty())
                .then(|| actors.iter().map(|actor| actor.bytes).sum()),
            context_impact_partial,
            actors,
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
            (failure(F::OperationDisabled), O::Blocked),
            (failure(F::ApprovalRequired), O::Blocked),
            (failure(F::AuthorityLost), O::Blocked),
            (failure(F::OutboundDestinationRejected), O::Blocked),
            (failure(F::ApprovalDeclined), O::NotAllowed),
            (failure(F::ApprovalExpired), O::NotAllowed),
            (failure(F::AccountConnectionDeclined), O::NotAllowed),
            (failure(F::AccountConnectionExpired), O::NotAllowed),
            (failure(F::AccountRequired), O::NeedsSetup),
            (failure(F::CredentialRefreshFailed), O::NeedsSetup),
            (failure(F::OAuthClientConfiguration), O::NeedsSetup),
            (failure(F::AccountAuthorizationChanged), O::NeedsSetup),
            (failure(F::CredentialRouteChanged), O::NeedsSetup),
            (failure(F::ProviderUnavailable), O::NeedsSetup),
            (failure(F::ProviderNamespaceConflict), O::NeedsSetup),
            (failure(F::ProviderPublicationFailed), O::NeedsSetup),
            (failure(F::OperationUnavailable), O::NeedsSetup),
            (failure(F::OperationNotFound), O::Failed),
            (failure(F::ProviderNotFound), O::Failed),
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
