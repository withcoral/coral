//! Client prompts for the invoking User's current human actions.
//!
//! An interactive client can ask its User about each current action of that
//! User's own Run. Answering an Approval Request prompt decides it through the
//! same Approval rules as Reef. An Account Connection Request prompt opens Reef's
//! connect page for its Provider: declining it declines the request, and
//! accepting it only opens the page. Prompts keep no state:
//! a still-pending action is offered again on a later observation. Agents and
//! other observers never receive prompts; Owners find Agent approvals in Reef.

use coral_db::Transaction;
use coral_workspace::{AuthenticatedPrincipal, ChangeContext, PrincipalKind, TraceId, WorkspaceId};

use crate::approval::ApprovalDecision;
use crate::lifecycle::RunRecord;
use crate::{
    ActionCursor, Error, RequiredAction, RequiredActionRequest, RunId, RunModule, required_actions,
    required_actions_repository, run_repository,
};

/// One current action that the invoking User can answer in their own client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionPrompt {
    /// The exact pending request this prompt answers.
    pub request: RequiredActionRequest,
    /// Stable, non-secret root-relative Reef path. Adapters resolve the Reef origin.
    pub run_status_path: String,
    /// When this Run stops waiting: the request's deadline or the Run's, whichever
    /// is first. Unix nanoseconds.
    pub deadline_at_unix_nanos: i64,
    /// Retained Tool path; empty when a shared connection spans different Tools.
    pub call_path: String,
    /// Current Provider display label, absent after the Provider is removed.
    pub provider_label: Option<String>,
    /// Root-relative Reef path of the User's own Account page, where one click
    /// connects it, for an Account Connection Request. It grants no authority.
    pub connect_path: Option<String>,
}

/// The User's answer to one prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromptAnswer {
    /// Approves the Invocation. Opening an Account Connection Request's page
    /// decides nothing.
    Accept,
    /// Declines the Invocation, or the Account Connection Request.
    Decline,
    /// Closes the prompt without an answer. The request stays pending for a
    /// later prompt or Reef.
    Dismiss,
}

impl RunModule {
    /// Returns a prompt for each current action of the caller's own Run.
    ///
    /// Only the invoking User receives prompts. Agents, other Users, and Owners
    /// observing Agent work receive none, as does a Run without current actions.
    /// Reading prompts changes nothing.
    ///
    /// # Errors
    /// An inaccessible Workspace returns [`Error::RunNotFound`]; persistence
    /// failures are also returned.
    pub async fn action_prompts(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
    ) -> Result<Vec<ActionPrompt>, Error> {
        let now = self.now()?;
        let mut tx = self.database.begin_read_snapshot().await?;
        let Some((run, actions)) = self
            .own_actions(&mut tx, actor, workspace_id, run_id, now)
            .await?
        else {
            return Ok(Vec::new());
        };
        let mut summaries = Vec::new();
        loop {
            let after =
                summaries
                    .last()
                    .map(|summary: &required_actions_repository::ActionEvidence| {
                        ActionCursor(summary.request.clone())
                    });
            let (page, _total, more) = required_actions_repository::page(
                &mut tx,
                workspace_id,
                &run.id,
                after.as_ref(),
                now,
                100,
            )
            .await?;
            summaries.extend(page);
            if !more {
                break;
            }
        }
        let mut prompts = Vec::with_capacity(actions.len());
        for action in actions {
            let summary = summaries
                .iter()
                .find(|summary| summary.request == action.request);
            let connect_path = match action.request {
                RequiredActionRequest::AccountConnection(id) => {
                    coral_provider::account::pending_connection_provider_in_transaction(
                        &mut tx,
                        workspace_id,
                        actor.principal(),
                        id,
                    )
                    .await?
                    .map(|provider| format!("/workspaces/{workspace_id}/accounts/{provider}"))
                }
                RequiredActionRequest::InvocationApproval(_) => None,
            };
            prompts.push(ActionPrompt {
                request: action.request,
                run_status_path: action.run_status_path,
                // A shared connection request can outlive this Run's wait.
                deadline_at_unix_nanos: action
                    .deadline_at_unix_nanos
                    .min(run.deadline_at_unix_nanos),
                call_path: summary
                    .map(|summary| summary.shared_call_path().to_owned())
                    .unwrap_or_default(),
                provider_label: summary.and_then(|summary| summary.provider_label.clone()),
                connect_path,
            });
        }
        tx.commit().await?;
        Ok(prompts)
    }

    /// Applies the invoking User's answer to one current action of their own Run.
    ///
    /// Accepting or declining an Approval Request prompt makes the same first-wins
    /// decision as Reef. Declining an Account Connection Request prompt declines
    /// the request, as in Reef; accepting it only opened the connect page.
    /// Dismissal decides nothing.
    ///
    /// # Errors
    /// A request that is not a current action of the caller's own Run returns
    /// [`Error::ApprovalNotFound`]; decision failures are also returned.
    pub async fn answer_action_prompt(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
        request: &RequiredActionRequest,
        answer: PromptAnswer,
        trace: &TraceId,
    ) -> Result<(), Error> {
        // Only a decline of a connection decides it; opening its page decides nothing.
        let decision = match (request, answer) {
            (_, PromptAnswer::Dismiss)
            | (RequiredActionRequest::AccountConnection(_), PromptAnswer::Accept) => return Ok(()),
            (_, PromptAnswer::Accept) => ApprovalDecision::Approve,
            (_, PromptAnswer::Decline) => ApprovalDecision::Decline,
        };
        let now = self.now()?;
        // Answer only a prompt the actor still sees. The decision rechecks its
        // own state under its locks.
        let mut tx = self.database.begin_read_snapshot().await?;
        let current = self
            .own_actions(&mut tx, actor, workspace_id, run_id, now)
            .await?
            .is_some_and(|(_run, actions)| actions.iter().any(|action| action.request == *request));
        tx.commit().await?;
        if !current {
            return Err(Error::ApprovalNotFound);
        }
        match request {
            RequiredActionRequest::InvocationApproval(id) => {
                self.decide_approval(actor, workspace_id, id, decision, None, trace)
                    .await?;
            }
            RequiredActionRequest::AccountConnection(id) => {
                // Only the invoking User is prompted, so this is their own request.
                // A connection request exists only when Accounts are composed.
                let (Some(accounts), Some(user)) = (&self.accounts, actor.authenticated_user())
                else {
                    return Ok(());
                };
                let context = ChangeContext::new(user.clone(), now, trace.clone());
                accounts
                    .decline_connection(&context, workspace_id, *id)
                    .await?;
            }
        }
        Ok(())
    }

    /// Whether the caller's own Account Connection Request ended connected, so an
    /// opened connect page can be announced complete. Anything else is `false`.
    pub async fn connection_connected(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        request: &RequiredActionRequest,
        trace: &TraceId,
    ) -> bool {
        let (RequiredActionRequest::AccountConnection(id), Some(accounts), Some(user), Ok(now)) = (
            request,
            &self.accounts,
            actor.authenticated_user(),
            self.now(),
        ) else {
            return false;
        };
        // The Account domain conceals another User's request as absent.
        let context = ChangeContext::new(user.clone(), now, trace.clone());
        accounts
            .get_connection_request(&context, workspace_id, *id)
            .await
            .is_ok_and(|request| {
                request.state == coral_provider::account::ConnectionState::Connected
            })
    }

    /// The caller's own Run and its current actions, or `None` for anyone else.
    async fn own_actions(
        &self,
        tx: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        run_id: &RunId,
        now: i64,
    ) -> Result<Option<(RunRecord, Vec<RequiredAction>)>, Error> {
        if actor.principal().kind() != PrincipalKind::User {
            return Ok(None);
        }
        self.visibility_scope_in_transaction(tx, actor, workspace_id)
            .await?;
        // Without Workspace Agent scope, a User finds only their own Runs.
        let Some(run) = run_repository::find_visible_in_transaction(
            tx,
            workspace_id,
            run_id,
            actor.principal(),
            false,
        )
        .await?
        else {
            return Ok(None);
        };
        let actions = required_actions::snapshot(tx, &run, actor.principal(), now).await?;
        Ok((!actions.is_empty()).then_some((run, actions)))
    }
}
