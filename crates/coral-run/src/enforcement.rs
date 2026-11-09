//! The Policy Enforcement Point's facts and decision (ADR 0018): one loader
//! reads the shared Workspace, Provider, policy and Account facts a batch of
//! ready Invocations depends on, under the lock mode the caller chooses, and
//! one evaluator decides each Invocation in memory against those facts, the
//! Run-scoped facts the owner holds, and one clock sample.
//!
//! The enforcement point loads the facts with a shared hold inside the owner's
//! chunk commit, so enforcement checks of different Runs read side by side and
//! an authority writer's `UPDATE` waits for the enforcement checks in flight: a
//! revocation that commits first is seen, one that commits after the recorded
//! dispatch start does not stop that dispatch. The parked-wait precheck loads
//! the same facts from a read snapshot with no hold and runs the same
//! evaluator, only to learn whether a held check could change its wait.

use std::collections::BTreeMap;

use coral_db::{RowLock, Transaction};
use coral_policy::{ToolAccessState, ToolPolicyTarget};
use coral_provider::account::{CredentialEvidence, RetainedAccountFact};
use coral_provider::{InvocationToolVerification, ResolvedInvocationTool};
use coral_workspace::{
    AuthenticatedPrincipal, Principal, PrincipalWorkspaceEvidence, WorkspaceId, WorkspaceModule,
};

use crate::invocation::AdmittedInvocation;
use crate::lifecycle::RunRecord;
use crate::{CoralFailure, Error};

/// Whose facts are read: the Run's authenticated actor in its Workspace.
pub(crate) struct EnforcementScope<'a> {
    pub(crate) workspaces: &'a WorkspaceModule,
    pub(crate) actor: &'a AuthenticatedPrincipal,
    pub(crate) workspace_id: &'a WorkspaceId,
}

/// One Invocation the facts are read for: its retained target and, once
/// selected, the credentials it will dispatch with.
#[derive(Clone, Copy)]
pub(crate) struct EnforcementTarget<'a> {
    pub(crate) admitted: &'a AdmittedInvocation,
    pub(crate) credential_evidence: Option<&'a CredentialEvidence>,
}

/// The Run-scoped facts a decision needs: the Run row the owner holds (its
/// claim, termination, deadline) and the identity the Invocation was
/// admitted under.
pub(crate) struct RunFacts<'a> {
    pub(crate) run: &'a RunRecord,
    pub(crate) claim_id: &'a str,
    pub(crate) principal: &'a Principal,
    pub(crate) deadline_at_unix_nanos: i64,
}

impl RunFacts<'_> {
    pub(crate) fn is_current(&self, now_unix_nanos: i64) -> bool {
        self.run
            .program_work_is_current(self.claim_id, now_unix_nanos)
            && self.run.principal == *self.principal
            && self.run.deadline_at_unix_nanos == self.deadline_at_unix_nanos
    }
}

/// An Invocation's Approval Request as the decision sees it, from owner
/// memory: in the enforcement point, what the held approval gate recorded (a
/// request is never needed again once approved, and an approval never changes);
/// in the parked-wait precheck, the pending request the Invocation is parked on
/// (only its owner decides it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum ApprovalFact {
    /// No request exists, or none was needed when the gate ran.
    #[default]
    NotRequested,
    Pending {
        deadline_at_unix_nanos: i64,
    },
    /// Approved at `decided_at_unix_nanos`. The approval counts if that is no
    /// later than the request's deadline, whenever the dispatch comes.
    Approved {
        decided_at_unix_nanos: i64,
        deadline_at_unix_nanos: i64,
    },
    Declined,
    Expired,
    Closed,
}

/// The Invocation-scoped facts beside the shared ones.
pub(crate) struct InvocationFacts<'a> {
    pub(crate) target: EnforcementTarget<'a>,
    /// Set while the Invocation is parked on a connection request that is
    /// still pending: when that request expires.
    pub(crate) connection_pending_until_unix_nanos: Option<i64>,
    pub(crate) approval: ApprovalFact,
}

/// What the evaluator decides for one Invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Decision {
    /// Every fact is current: record the dispatch start.
    Dispatch,
    /// A fact refuses the dispatch; the Invocation ends with this failure.
    Deny(CoralFailure),
    /// The Account's material must be renewed before dispatch; the
    /// Invocation parks on the refresh and meets the enforcement point again.
    RefreshRequired,
    /// The Invocation keeps waiting (for a connection or a decision) until
    /// this instant at the latest, when nothing commits before.
    Wait { until_unix_nanos: i64 },
    /// The Run no longer takes Program work.
    RunTerminated,
}

/// What one Tool verdict is keyed by: the retained Generation
/// (immutable), the Tool key, and the credential route and
/// account-setting version the verification compares with the current
/// Provider. Targets that retained different access share no verdict.
type ToolVerdictKey = (
    coral_catalog::ProviderGenerationId,
    coral_catalog::ToolKey,
    coral_provider::CredentialRoute,
    i64,
);
/// The selected Account and the authorization the Invocation retained.
type AccountKey = (coral_provider::account::ProviderAccountId, String);

/// The facts a batch of Invocations shares, read once per batch.
pub(crate) struct SharedFacts {
    authority: Result<PrincipalWorkspaceEvidence, CoralFailure>,
    /// Per retained Tool: the policy the Invocation runs under, once
    /// the Provider and the Tool are current.
    tools: BTreeMap<ToolVerdictKey, Result<ToolAccessState, CoralFailure>>,
    /// Per selected Account: what the dispatch decides the Account on, once
    /// it is current.
    accounts: BTreeMap<AccountKey, Result<RetainedAccountFact, CoralFailure>>,
}

fn verdict_key(target: &ResolvedInvocationTool) -> ToolVerdictKey {
    let access = target.provider_access();
    (
        target.provider_generation_id().clone(),
        target.tool().key().clone(),
        access.credential_route(),
        access.version(),
    )
}

fn account_key(evidence: &CredentialEvidence) -> AccountKey {
    (evidence.account_id, evidence.authorization_version.clone())
}

pub(crate) fn policy_target(
    workspace_id: &WorkspaceId,
    target: &ResolvedInvocationTool,
) -> ToolPolicyTarget {
    ToolPolicyTarget::new(
        workspace_id.clone(),
        target.provider_access().id().clone(),
        target.tool().key().clone(),
    )
}

impl SharedFacts {
    /// Reads the facts `targets` depend on, each distinct fact once, holding
    /// their rows as `lock` says, in one order: the actor's authority (the
    /// User's Organization row and the Workspace row), then each distinct
    /// Provider and Tool with its policy, then each distinct Account.
    /// `now_unix_nanos` dates the reads that are themselves time-bound (an
    /// Agent Registration); the decision samples the clock again after the
    /// last read.
    pub(crate) async fn load(
        transaction: &mut Transaction<'_>,
        lock: RowLock,
        scope: &EnforcementScope<'_>,
        now_unix_nanos: i64,
        targets: &[EnforcementTarget<'_>],
    ) -> Result<Self, Error> {
        let authority = match scope
            .workspaces
            .check_dispatch_in_transaction(
                transaction,
                scope.actor,
                scope.workspace_id,
                now_unix_nanos,
                lock,
            )
            .await
        {
            Ok(evidence) => Ok(evidence),
            Err(error) => Err(authority_failure(error)?),
        };
        let mut tools: BTreeMap<ToolVerdictKey, &ResolvedInvocationTool> = BTreeMap::new();
        let mut accounts: BTreeMap<AccountKey, (&ResolvedInvocationTool, &CredentialEvidence)> =
            BTreeMap::new();
        for target in targets {
            tools
                .entry(verdict_key(&target.admitted.target))
                .or_insert(&target.admitted.target);
            if let Some(evidence) = target.credential_evidence {
                accounts
                    .entry(account_key(evidence))
                    .or_insert((&target.admitted.target, evidence));
            }
        }
        // A Private Tool is the actor's own, so the actor's role decides
        // whether an Owner's rule on it applies. Lost authority refuses
        // every target, whatever the rule.
        let owner = coral_policy::PrivateToolOwner::of(
            scope.actor.principal().kind(),
            authority
                .as_ref()
                .ok()
                .and_then(PrincipalWorkspaceEvidence::effective_user_role),
        );
        let mut tool_facts = BTreeMap::new();
        for (key, target) in tools {
            let fact = tool_fact(transaction, lock, scope.workspace_id, target, owner).await?;
            tool_facts.insert(key, fact);
        }
        let mut account_facts = BTreeMap::new();
        for (key, (target, evidence)) in accounts {
            let fact = match coral_provider::account::retained_account_fact_in_transaction(
                transaction,
                target,
                evidence,
                lock,
            )
            .await
            {
                Ok(fact) => Ok(fact),
                Err(error) => Err(crate::invocation::account_failure(error)?),
            };
            account_facts.insert(key, fact);
        }
        Ok(Self {
            authority,
            tools: tool_facts,
            accounts: account_facts,
        })
    }

    /// The policy `target` runs under, once the Run, the actor's authority,
    /// the Provider and Tool, and the selected Account are current at
    /// `now_unix_nanos`; otherwise what refuses it.
    pub(crate) fn checked(
        &self,
        run: &RunFacts<'_>,
        target: EnforcementTarget<'_>,
        now_unix_nanos: i64,
    ) -> Result<Checked, Refusal> {
        if !run.is_current(now_unix_nanos) {
            return Err(Refusal::RunTerminated);
        }
        let authority = match &self.authority {
            Ok(authority) => authority,
            Err(failure) => return Err(Refusal::Deny(*failure)),
        };
        if authority.require_current_at(now_unix_nanos).is_err() {
            return Err(Refusal::Deny(CoralFailure::AuthorityLost));
        }
        let policy = match self.tools.get(&verdict_key(&target.admitted.target)) {
            Some(Ok(policy)) => *policy,
            Some(Err(failure)) => return Err(Refusal::Deny(*failure)),
            None => return Err(Refusal::Deny(CoralFailure::AuthorityLost)),
        };
        if policy == ToolAccessState::Disabled {
            return Err(Refusal::Deny(CoralFailure::ToolDisabled));
        }
        let account = match target.credential_evidence {
            None => None,
            Some(evidence) => match self.accounts.get(&account_key(evidence)) {
                Some(Ok(fact)) => Some(*fact),
                Some(Err(failure)) => return Err(Refusal::Deny(*failure)),
                None => return Err(Refusal::Deny(CoralFailure::AccountRequired)),
            },
        };
        Ok(Checked {
            policy,
            account,
            until_unix_nanos: authority
                .expires_at_unix_nanos()
                .map_or(run.deadline_at_unix_nanos, |expiry| {
                    expiry.min(run.deadline_at_unix_nanos)
                }),
        })
    }
}

/// What refuses an Invocation before any wait or approval is considered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The Run no longer takes Program work.
    RunTerminated,
    /// A fact refuses the dispatch; the Invocation ends with this failure.
    Deny(CoralFailure),
}

impl From<Refusal> for Decision {
    fn from(refusal: Refusal) -> Self {
        match refusal {
            Refusal::RunTerminated => Self::RunTerminated,
            Refusal::Deny(failure) => Self::Deny(failure),
        }
    }
}

/// The shared facts found current for one Invocation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Checked {
    pub(crate) policy: ToolAccessState,
    account: Option<RetainedAccountFact>,
    /// When the Run or the actor's authority expires without a commit.
    until_unix_nanos: i64,
}

/// Decides one Invocation against the facts at one instant: the Run, then
/// the shared facts, then the wait it may still be in, then its approval
/// under the policy, then whether its credentials must be renewed first.
/// An approved request is judged by its two recorded times, never by the
/// instant: the Run's own deadline bounds a dispatch that comes after the
/// request's.
pub(crate) fn decide(
    shared: &SharedFacts,
    run: &RunFacts<'_>,
    invocation: &InvocationFacts<'_>,
    now_unix_nanos: i64,
) -> Decision {
    let checked = match shared.checked(run, invocation.target, now_unix_nanos) {
        Ok(checked) => checked,
        Err(refusal) => return refusal.into(),
    };
    if let Some(expires_at) = invocation.connection_pending_until_unix_nanos {
        return Decision::Wait {
            until_unix_nanos: expires_at.min(checked.until_unix_nanos),
        };
    }
    if checked.policy == ToolAccessState::ApprovalRequired {
        match invocation.approval {
            ApprovalFact::Approved {
                decided_at_unix_nanos,
                deadline_at_unix_nanos,
            } if decided_at_unix_nanos <= deadline_at_unix_nanos => {}
            ApprovalFact::Pending {
                deadline_at_unix_nanos,
            } if now_unix_nanos < deadline_at_unix_nanos => {
                return Decision::Wait {
                    until_unix_nanos: deadline_at_unix_nanos.min(checked.until_unix_nanos),
                };
            }
            ApprovalFact::Approved { .. }
            | ApprovalFact::Pending { .. }
            | ApprovalFact::Expired => {
                return Decision::Deny(CoralFailure::ApprovalExpired);
            }
            ApprovalFact::Declined => return Decision::Deny(CoralFailure::ApprovalDeclined),
            ApprovalFact::NotRequested | ApprovalFact::Closed => {
                return Decision::Deny(CoralFailure::ApprovalRequired);
            }
        }
    }
    if checked
        .account
        .is_some_and(|account| account.needs_refresh_at(now_unix_nanos))
    {
        return Decision::RefreshRequired;
    }
    Decision::Dispatch
}

/// The policy one retained Tool runs under, once its Provider and the
/// Tool row are current; the failure that refuses it otherwise. A Private
/// Tool's is decided for `owner`, whose Tool it is.
async fn tool_fact(
    transaction: &mut Transaction<'_>,
    lock: RowLock,
    workspace_id: &WorkspaceId,
    target: &ResolvedInvocationTool,
    owner: coral_policy::PrivateToolOwner,
) -> Result<Result<ToolAccessState, CoralFailure>, Error> {
    match coral_provider::verify_invocation_tool_in_transaction(
        transaction,
        workspace_id,
        target,
        lock,
    )
    .await?
    {
        InvocationToolVerification::Current => {}
        InvocationToolVerification::CredentialRouteChanged => {
            return Ok(Err(CoralFailure::CredentialRouteChanged));
        }
        InvocationToolVerification::AuthorityLost => {
            return Ok(Err(CoralFailure::AuthorityLost));
        }
    }
    match tool_decision_in_transaction(transaction, workspace_id, target, owner, lock).await {
        Ok(decision) => Ok(Ok(decision.access_state())),
        Err(coral_policy::Error::ToolNotFound) => Ok(Err(CoralFailure::AuthorityLost)),
        Err(error) => Err(error.into()),
    }
}

/// The decision for `target` now, holding its exact rule as `lock` says: a
/// Provider Tool's from its retained Generation's facts, and a Private
/// Tool's from the default its author declared for the linked revision,
/// for `owner`.
pub(crate) async fn tool_decision_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    target: &ResolvedInvocationTool,
    owner: coral_policy::PrivateToolOwner,
    lock: RowLock,
) -> Result<coral_policy::ToolPolicyDecision, coral_policy::Error> {
    let policy_target = policy_target(workspace_id, target);
    let facts = target.tool().policy_facts();
    match (target.tool().execution_binding(), facts.declared_default()) {
        (coral_catalog::ExecutionBinding::Code(_), Some(declared)) => {
            coral_policy::private_tool_decision_in_transaction(
                transaction,
                &policy_target,
                declared,
                owner,
                lock,
            )
            .await
        }
        (coral_catalog::ExecutionBinding::Code(_), None) => Err(coral_policy::Error::CorruptData(
            "a Private Tool declares no default".to_owned(),
        )),
        _ => {
            coral_policy::invocation_decision_in_transaction(
                transaction,
                &policy_target,
                facts,
                lock,
            )
            .await
        }
    }
}

/// The known failure a Workspace authority error means (lost authority), or
/// the error itself when it is not one.
pub(crate) fn authority_failure(error: coral_workspace::Error) -> Result<CoralFailure, Error> {
    if loses_authority(&error) {
        Ok(CoralFailure::AuthorityLost)
    } else {
        Err(error.into())
    }
}

/// Whether a Workspace error means the Principal lost authority, rather than
/// a host defect.
pub(crate) const fn loses_authority(error: &coral_workspace::Error) -> bool {
    matches!(
        error,
        coral_workspace::Error::WorkspaceNotFound
            | coral_workspace::Error::PermissionDenied(_)
            | coral_workspace::Error::WorkspaceArchived
            | coral_workspace::Error::AgentNotFound
            | coral_workspace::Error::AgentDisabled
            | coral_workspace::Error::AgentRegistrationExpired
    )
}
