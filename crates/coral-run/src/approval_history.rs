//! A Workspace's settled Approval Requests, newest decision first. Each page
//! lists requests the caller can already inspect, matched by who invoked the
//! Run, and carries no arguments, comments or decision source.

use std::collections::HashMap;

use coral_db::Transaction;
use coral_workspace::{AuthenticatedPrincipal, HistoryAuthority, Principal};

use crate::Error;
use crate::approval::{ApprovalId, ApprovalState};

/// How a settled request ended. A request Coral closed is not settled here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettledOutcome {
    /// A person approved it.
    Approved,
    /// A person declined it.
    Declined,
    /// Its deadline passed with nobody deciding.
    Expired,
}

/// The person whose decision won.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Approver {
    /// The deciding User.
    pub principal: Principal,
    /// Their current name, when Workspace profile rules let the caller read it.
    pub name: Option<String>,
}

/// One settled request with the Run it belonged to.
#[derive(Debug)]
pub struct SettledApproval<R = crate::RunSummary> {
    /// Opaque, non-authorizing request identity.
    pub approval: ApprovalId,
    /// How it ended.
    pub outcome: SettledOutcome,
    /// When it settled: the decision, or the deadline of an expired request.
    pub settled_at_unix_nanos: i64,
    /// Absent for an expired request.
    pub approver: Option<Approver>,
    /// Retained Tool path.
    pub call_path: String,
    /// Current Provider display label, absent after the Provider is removed.
    pub provider_label: Option<String>,
    /// The Run that asked. Callers that attach their own evidence keep this shape.
    pub run: R,
}

/// Non-authorizing position in the newest-first history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettledCursor {
    pub(crate) settled_at_unix_nanos: i64,
    pub(crate) approval: ApprovalId,
}

impl SettledCursor {
    /// Parses the opaque continuation returned with a history page.
    /// # Errors
    /// Rejects malformed positions.
    pub fn parse(value: &str) -> Result<Self, Error> {
        value
            .split_once('/')
            .and_then(|(at, approval)| {
                Some(Self {
                    settled_at_unix_nanos: at.parse().ok()?,
                    approval: ApprovalId::parse(approval).ok()?,
                })
            })
            .ok_or_else(|| Error::InvalidInput("invalid approval history cursor".into()))
    }
}

impl std::fmt::Display for SettledCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.settled_at_unix_nanos, self.approval)
    }
}

/// One page of history, newest first.
#[derive(Debug)]
pub struct SettledApprovals<R = crate::RunSummary> {
    /// This page.
    pub approvals: Vec<SettledApproval<R>>,
    /// Continuation after this page, absent at the end.
    pub next: Option<SettledCursor>,
}

impl crate::RunModule {
    /// Lists settled requests of Runs `authority` covers, where `authority`
    /// came from `action_history_authority_in_transaction` in this snapshot:
    /// the caller's own Runs, plus Agent Runs in Workspaces the caller owns.
    /// `actor` names each approver only as profile rules let it.
    /// # Errors
    /// Rejects page sizes outside 1–100.
    pub async fn settled_approvals_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        authority: &HistoryAuthority,
        after: Option<&SettledCursor>,
        limit: u16,
    ) -> Result<SettledApprovals, Error> {
        if !(1..=100).contains(&limit) {
            return Err(Error::InvalidInput(
                "invalid approval history page size".into(),
            ));
        }
        let now = self.now()?;
        let (rows, has_more) =
            crate::approval_history_repository::page(tx, authority, after, limit).await?;
        let mut names = HashMap::new();
        let mut approvals = Vec::with_capacity(rows.len());
        for row in rows {
            let approver = if let Some(principal) = row.approver {
                // A page often repeats an approver; each is read once.
                let key = (row.workspace.to_string(), principal.id().to_string());
                if !names.contains_key(&key) {
                    let name = crate::approval::approver_name(
                        tx,
                        &self.workspaces,
                        actor,
                        &row.workspace,
                        &principal,
                    )
                    .await?;
                    names.insert(key.clone(), name);
                }
                let name = names.get(&key).cloned().flatten();
                Some(Approver { principal, name })
            } else {
                None
            };
            approvals.push(SettledApproval {
                run: crate::history_repository::run_summary(
                    tx,
                    authority,
                    &row.workspace,
                    &row.run,
                    now,
                )
                .await?,
                approval: row.approval,
                outcome: outcome(row.state)?,
                settled_at_unix_nanos: row.settled_at_unix_nanos,
                approver,
                call_path: row.call_path,
                provider_label: row.provider_label,
            });
        }
        let next = approvals
            .last()
            .filter(|_| has_more)
            .map(|last| SettledCursor {
                settled_at_unix_nanos: last.settled_at_unix_nanos,
                approval: last.approval.clone(),
            });
        Ok(SettledApprovals { approvals, next })
    }
}

fn outcome(state: ApprovalState) -> Result<SettledOutcome, Error> {
    match state {
        ApprovalState::Approved => Ok(SettledOutcome::Approved),
        ApprovalState::Declined => Ok(SettledOutcome::Declined),
        ApprovalState::Expired => Ok(SettledOutcome::Expired),
        ApprovalState::Pending | ApprovalState::Closed => {
            Err(Error::CorruptData("unsettled approval in history".into()))
        }
    }
}
