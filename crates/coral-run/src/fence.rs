//! The Dispatch Fence's facts and decision (ADR 0018): one loader reads the
//! shared Workspace, Provider, policy and Account facts a batch of ready
//! Invocations depends on, under the lock mode the caller chooses, and one
//! evaluator decides each Invocation in memory against those facts, the
//! Run-scoped facts the owner holds, and one clock sample.
//!
//! The fence loads the facts with a shared hold inside the owner's chunk
//! commit, so fences of different Runs read side by side and an authority
//! writer's `UPDATE` waits for the fences in flight: a revocation that commits
//! first is seen, one that commits after the recorded dispatch start does not
//! stop that dispatch. The parked-wait precheck loads the same facts from a
//! read snapshot with no hold and runs the same evaluator, only to learn
//! whether a held check could change its wait.

use std::collections::BTreeMap;

use coral_db::{RowLock, Transaction};
use coral_policy::{OperationAccessState, OperationPolicyTarget};
use coral_provider::account::{CredentialEvidence, RetainedAccountFact};
use coral_provider::{InvocationOperationVerification, ResolvedInvocationOperation};
use coral_workspace::{
    AuthenticatedPrincipal, Principal, PrincipalWorkspaceEvidence, WorkspaceId, WorkspaceModule,
};

use crate::invocation::AdmittedInvocation;
use crate::lifecycle::RunRecord;
use crate::{CoralFailure, Error};

/// Whose facts are read: the Run's authenticated actor in its Workspace.
pub(crate) struct FenceScope<'a> {
    pub(crate) workspaces: &'a WorkspaceModule,
    pub(crate) actor: &'a AuthenticatedPrincipal,
    pub(crate) workspace_id: &'a WorkspaceId,
}

/// One Invocation the facts are read for: its retained target and, once
/// selected, the credentials it will dispatch with.
#[derive(Clone, Copy)]
pub(crate) struct FenceTarget<'a> {
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
/// memory: in the fence, what the held approval gate recorded (a request is
/// never needed again once approved, and an approval never changes); in the
/// parked-wait precheck, the pending request the Invocation is parked on
/// (only its owner decides it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum ApprovalFact {
    /// No request exists, or none was needed when the gate ran.
    #[default]
    NotRequested,
    Pending {
        deadline_at_unix_nanos: i64,
    },
    Approved {
        deadline_at_unix_nanos: i64,
    },
    Declined,
    Expired,
    Closed,
}

/// The Invocation-scoped facts beside the shared ones.
pub(crate) struct InvocationFacts<'a> {
    pub(crate) target: FenceTarget<'a>,
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
    /// Invocation parks on the refresh and fences again.
    RefreshRequired,
    /// The Invocation keeps waiting (for a connection or a decision) until
    /// this instant at the latest, when nothing commits before.
    Wait { until_unix_nanos: i64 },
    /// The Run no longer takes Program work.
    RunTerminated,
}

/// What one Operation verdict is keyed by: the retained Generation
/// (immutable), the Operation key, and the credential route and
/// account-setting version the verification compares with the current
/// Provider. Targets that retained different access share no verdict.
type OperationVerdictKey = (
    coral_catalog::ProviderGenerationId,
    coral_catalog::OperationKey,
    coral_provider::CredentialRoute,
    i64,
);
/// The selected Account and the authorization the Invocation retained.
type AccountKey = (coral_provider::account::ProviderAccountId, String);

/// The facts a batch of Invocations shares, read once per batch.
pub(crate) struct SharedFacts {
    authority: Result<PrincipalWorkspaceEvidence, CoralFailure>,
    /// Per retained Operation: the policy the Invocation runs under, once
    /// the Provider and the Operation are current.
    operations: BTreeMap<OperationVerdictKey, Result<OperationAccessState, CoralFailure>>,
    /// Per selected Account: what the dispatch decides the Account on, once
    /// it is current.
    accounts: BTreeMap<AccountKey, Result<RetainedAccountFact, CoralFailure>>,
}

fn verdict_key(target: &ResolvedInvocationOperation) -> OperationVerdictKey {
    let access = target.provider_access();
    (
        target.provider_generation_id().clone(),
        target.operation().key().clone(),
        access.credential_route(),
        access.version(),
    )
}

fn account_key(evidence: &CredentialEvidence) -> AccountKey {
    (evidence.account_id, evidence.authorization_version.clone())
}

pub(crate) fn policy_target(
    workspace_id: &WorkspaceId,
    target: &ResolvedInvocationOperation,
) -> OperationPolicyTarget {
    OperationPolicyTarget::new(
        workspace_id.clone(),
        target.provider_access().id().clone(),
        target.operation().key().clone(),
    )
}

impl SharedFacts {
    /// Reads the facts `targets` depend on, each distinct fact once, holding
    /// their rows as `lock` says, in one order: the actor's authority (the
    /// User's Organization row and the Workspace row), then each distinct
    /// Provider and Operation with its policy, then each distinct Account.
    /// `now_unix_nanos` dates the reads that are themselves time-bound (an
    /// Agent Registration); the decision samples the clock again after the
    /// last read.
    pub(crate) async fn load(
        transaction: &mut Transaction<'_>,
        lock: RowLock,
        scope: &FenceScope<'_>,
        now_unix_nanos: i64,
        targets: &[FenceTarget<'_>],
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
        let mut operations: BTreeMap<OperationVerdictKey, &ResolvedInvocationOperation> =
            BTreeMap::new();
        let mut accounts: BTreeMap<
            AccountKey,
            (&ResolvedInvocationOperation, &CredentialEvidence),
        > = BTreeMap::new();
        for target in targets {
            operations
                .entry(verdict_key(&target.admitted.target))
                .or_insert(&target.admitted.target);
            if let Some(evidence) = target.credential_evidence {
                accounts
                    .entry(account_key(evidence))
                    .or_insert((&target.admitted.target, evidence));
            }
        }
        let mut operation_facts = BTreeMap::new();
        for (key, target) in operations {
            let fact = operation_fact(transaction, lock, scope.workspace_id, target).await?;
            operation_facts.insert(key, fact);
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
            operations: operation_facts,
            accounts: account_facts,
        })
    }

    /// The policy `target` runs under, once the Run, the actor's authority,
    /// the Provider and Operation, and the selected Account are current at
    /// `now_unix_nanos`; otherwise what refuses it.
    pub(crate) fn checked(
        &self,
        run: &RunFacts<'_>,
        target: FenceTarget<'_>,
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
        let policy = match self.operations.get(&verdict_key(&target.admitted.target)) {
            Some(Ok(policy)) => *policy,
            Some(Err(failure)) => return Err(Refusal::Deny(*failure)),
            None => return Err(Refusal::Deny(CoralFailure::AuthorityLost)),
        };
        if policy == OperationAccessState::Disabled {
            return Err(Refusal::Deny(CoralFailure::OperationDisabled));
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
    pub(crate) policy: OperationAccessState,
    account: Option<RetainedAccountFact>,
    /// When the Run or the actor's authority expires without a commit.
    until_unix_nanos: i64,
}

/// Decides one Invocation against the facts at one instant: the Run, then
/// the shared facts, then the wait it may still be in, then its approval
/// under the policy, then whether its credentials must be renewed first.
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
    if checked.policy == OperationAccessState::ApprovalRequired {
        match invocation.approval {
            ApprovalFact::Approved {
                deadline_at_unix_nanos,
            } if now_unix_nanos < deadline_at_unix_nanos => {}
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

/// The policy one retained Operation runs under, once its Provider and the
/// Operation row are current; the failure that refuses it otherwise.
async fn operation_fact(
    transaction: &mut Transaction<'_>,
    lock: RowLock,
    workspace_id: &WorkspaceId,
    target: &ResolvedInvocationOperation,
) -> Result<Result<OperationAccessState, CoralFailure>, Error> {
    match coral_provider::verify_invocation_operation_in_transaction(
        transaction,
        workspace_id,
        target,
        lock,
    )
    .await?
    {
        InvocationOperationVerification::Current => {}
        InvocationOperationVerification::CredentialRouteChanged => {
            return Ok(Err(CoralFailure::CredentialRouteChanged));
        }
        InvocationOperationVerification::AuthorityLost => {
            return Ok(Err(CoralFailure::AuthorityLost));
        }
    }
    match coral_policy::invocation_decision_in_transaction(
        transaction,
        &policy_target(workspace_id, target),
        target.operation().hints(),
        target.operation().http_method(),
        lock,
    )
    .await
    {
        Ok(decision) => Ok(Ok(decision.access_state())),
        Err(coral_policy::Error::OperationNotFound) => Ok(Err(CoralFailure::AuthorityLost)),
        Err(error) => Err(error.into()),
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
