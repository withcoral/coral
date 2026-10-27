//! Exact Workspace Tool policy for Coral.
//!
//! This private domain Module is the transport-neutral Interface for deciding
//! and changing one Tool Access State addressed by an exact Workspace,
//! Provider, and Tool Key. It supports only `enabled`,
//! `approval_required`, and `disabled`; no Principal, Account, Credential
//! Route, ordered, pattern, or glob dimensions enter the Interface.
//!
//! # Interface
//!
//! - [`ToolPolicyModule::decision`] returns the current exact decision.
//! - [`ToolPolicyModule::decisions`] reads a Provider's bounded selection in one snapshot.
//! - [`ToolPolicyModule::provider_rules`] reads one Provider's exact rules
//!   by Call Path for any Principal that inspects the Workspace.
//! - [`ToolPolicyModule::set_access_state`] creates or changes one exact
//!   rule for a Workspace Owner.
//! - [`ToolPolicyModule::set_access_states`] atomically changes 1–100 distinct
//!   exact decisions in one Workspace with their lifecycle evidence.
//! - [`ToolPolicyModule::set_provider_access_states`] atomically changes
//!   1–100 exact decisions of one Provider, each named by its Call Path.
//! - [`discovery_decision_in_transaction`] reads one exact current decision
//!   without a lock or a policy write, as when Run inspects an approval.
//! - [`exact_tool_rules_in_transaction`] reads every exact rule of one
//!   Workspace, so discovery decides all its Tools from one read.
//! - [`invocation_decision_in_transaction`] rechecks an exact retained decision
//!   inside Run-owned admission and Dispatch Fence transactions, holding the
//!   explicit rule shared when asked to.
//! - [`ToolPolicyModule::list_lifecycle_events`] exposes safe policy-domain
//!   Administrative Lifecycle Events.
//!
//! # Invariants
//!
//! The current active Provider catalog proves that the exact target exists.
//! Without an exact rule, a declared read-only Tool and an HTTP Tool
//! with a safe method are enabled; every other Tool is approval-required.
//! A policy change and its safe lifecycle Event commit in one transaction.
//!
//! The Module reads no process environment. Adapters supply authenticated User,
//! timestamp, trace, and exact target values explicitly. Persistence rows and
//! transport envelopes do not cross the Interface.

mod error;
pub mod history_repository;
mod repository;
mod types;

use std::collections::BTreeMap;
use std::sync::Arc;

use coral_catalog::{HttpMethod, ToolHints, ToolKey};
use coral_db::Database;
use coral_provider::ProviderNamespace;
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, ChangeContext, WorkspaceAction, WorkspaceEvidence,
    WorkspaceId, WorkspaceModule, WorkspaceRole,
};

pub use error::Error;
pub use types::{
    AdministrativeLifecycleEvent, ExactToolRules, LifecycleAction, LifecycleOutcome,
    ToolAccessState, ToolPolicyDecision, ToolPolicyTarget,
};

/// Exact Workspace Tool-policy Module.
#[derive(Clone)]
pub struct ToolPolicyModule {
    database: Arc<Database>,
    workspaces: WorkspaceModule,
}

impl ToolPolicyModule {
    /// Builds the Module from initialized persistence and Workspace authority.
    #[must_use]
    pub const fn new(database: Arc<Database>, workspaces: WorkspaceModule) -> Self {
        Self {
            database,
            workspaces,
        }
    }

    /// Returns the effective decision for one exact current Tool target.
    ///
    /// # Errors
    ///
    /// Returns an authority, absent-target, corrupt-data, or persistence error.
    pub async fn decision(
        &self,
        actor: &AuthenticatedUser,
        target: &ToolPolicyTarget,
    ) -> Result<ToolPolicyDecision, Error> {
        self.decisions(actor, std::slice::from_ref(target))
            .await?
            .pop()
            .ok_or(Error::InvalidBatch)
    }

    /// Returns 1–100 distinct current decisions from one Provider in request order.
    /// Authority, current catalog hints and explicit rules share one read snapshot.
    /// # Errors
    /// Rejects invalid batches, absent targets, lost authority and persistence failures.
    pub async fn decisions(
        &self,
        actor: &AuthenticatedUser,
        targets: &[ToolPolicyTarget],
    ) -> Result<Vec<ToolPolicyDecision>, Error> {
        let first = targets.first().ok_or(Error::InvalidBatch)?;
        let mut seen = std::collections::HashSet::new();
        if targets.len() > 100
            || targets.iter().any(|target| {
                target.workspace_id() != first.workspace_id()
                    || target.provider_id() != first.provider_id()
                    || !seen.insert(target)
            })
        {
            return Err(Error::InvalidBatch);
        }
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.workspaces
            .check_in_transaction(
                &mut transaction,
                actor,
                first.workspace_id(),
                WorkspaceAction::Inspect,
            )
            .await?;
        let keys: Vec<_> = targets
            .iter()
            .map(|target| target.tool_key().clone())
            .collect();
        let hints_and_methods = coral_provider::tool_hints_and_methods_batch_in_transaction(
            &mut transaction,
            first.workspace_id(),
            first.provider_id(),
            &keys,
        )
        .await?;
        let explicit = repository::access_states_in_transaction(
            &mut transaction,
            first.workspace_id(),
            first.provider_id(),
            &keys,
        )
        .await?;
        let decisions = targets
            .iter()
            .map(|target| {
                let (hints, http_method) = hints_and_methods
                    .get(target.tool_key())
                    .ok_or(Error::ToolNotFound)?;
                Ok(resolved_decision(
                    target,
                    *hints,
                    *http_method,
                    explicit.get(target.tool_key()).copied(),
                ))
            })
            .collect::<Result<_, Error>>()?;
        transaction.rollback().await?;
        Ok(decisions)
    }

    /// Returns the exact rules a Workspace Owner set for the Provider under
    /// `coral.providers.<namespace>`, in any state, keyed by each Tool's
    /// Call Path relative to that Call Root. A Tool without a rule takes
    /// its default and is left out, and so is a rule whose Tool left the
    /// current catalog. Every Principal that inspects the Workspace reads them.
    ///
    /// # Errors
    ///
    /// Returns lost authority, an absent Provider, or a persistence failure.
    pub async fn provider_rules(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        namespace: &ProviderNamespace,
        now_unix_nanos: i64,
    ) -> Result<BTreeMap<String, ToolAccessState>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.workspaces
            .check_principal_inspection_in_snapshot(
                &mut transaction,
                actor,
                workspace_id,
                now_unix_nanos,
            )
            .await?;
        let (provider, paths) = coral_provider::provider_tool_paths_in_transaction(
            &mut transaction,
            workspace_id,
            namespace,
        )
        .await?
        .ok_or(coral_provider::Error::ProviderNotFound)?;
        let mut rules =
            repository::provider_rules_in_transaction(&mut transaction, workspace_id, &provider)
                .await?;
        transaction.rollback().await?;
        Ok(paths
            .iter()
            .filter_map(|path| {
                rules
                    .remove(path.key())
                    .map(|state| (path.relative_path().to_owned(), state))
            })
            .collect())
    }

    /// Creates or changes one exact Tool Access State.
    ///
    /// Repeating an already explicit state is a no-op and writes no duplicate
    /// lifecycle evidence.
    ///
    /// # Errors
    ///
    /// Requires an active Workspace and Workspace Owner authority.
    pub async fn set_access_state(
        &self,
        context: &ChangeContext,
        target: ToolPolicyTarget,
        access_state: ToolAccessState,
    ) -> Result<ToolPolicyDecision, Error> {
        self.set_access_states(context, vec![(target, access_state)])
            .await?
            .pop()
            .ok_or(Error::InvalidBatch)
    }

    /// Atomically changes 1–100 distinct exact decisions in one Workspace.
    /// A rejected target leaves every decision and lifecycle Event unchanged.
    /// Repeating explicit states is a no-op, including their lifecycle evidence.
    /// # Errors
    /// Rejects empty, oversized, duplicate, cross-Workspace, absent or unauthorized targets.
    pub async fn set_access_states(
        &self,
        context: &ChangeContext,
        updates: Vec<(ToolPolicyTarget, ToolAccessState)>,
    ) -> Result<Vec<ToolPolicyDecision>, Error> {
        let workspace = updates.first().ok_or(Error::InvalidBatch)?.0.workspace_id();
        let mut seen = std::collections::HashSet::new();
        if updates.len() > 100
            || updates
                .iter()
                .any(|(target, _)| target.workspace_id() != workspace || !seen.insert(target))
        {
            return Err(Error::InvalidBatch);
        }
        let mut transaction = self.database.begin().await?;
        let evidence = self
            .workspaces
            .check_configuration_in_transaction(&mut transaction, context.actor(), workspace)
            .await?;
        let mut decisions = Vec::with_capacity(updates.len());
        for (target, access_state) in updates {
            require_tool_in_transaction(&mut transaction, &target).await?;
            decisions.push(
                write_access_state_in_transaction(
                    &mut transaction,
                    context,
                    &evidence,
                    target,
                    access_state,
                )
                .await?,
            );
        }
        transaction.commit().await?;
        Ok(decisions)
    }

    /// Atomically sets the exact state of 1–100 Tools of the Provider
    /// under `coral.providers.<namespace>`, in any state, each named by its
    /// Call Path relative to that Call Root. Each path resolves to its
    /// Tool in the transaction that writes its rule, so an update cannot
    /// give the state to another Tool. A rejected path leaves every
    /// decision and lifecycle Event unchanged, and a repeated state writes
    /// nothing. Returns each path's state.
    ///
    /// # Errors
    ///
    /// Rejects an empty or oversized batch, lost Owner authority, an absent
    /// Provider, and every path that names no Tool of its current
    /// catalog.
    pub async fn set_provider_access_states(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        namespace: &ProviderNamespace,
        states: &BTreeMap<String, ToolAccessState>,
    ) -> Result<BTreeMap<String, ToolAccessState>, Error> {
        if states.is_empty() || states.len() > 100 {
            return Err(Error::InvalidBatch);
        }
        let mut transaction = self.database.begin().await?;
        // The Workspace hold this takes serializes the change with every
        // Provider update in the Workspace, which takes it too, so each path
        // names one Tool until the commit.
        let evidence = self
            .workspaces
            .check_configuration_in_transaction(&mut transaction, context.actor(), workspace_id)
            .await?;
        let (provider, paths) = coral_provider::provider_tool_paths_in_transaction(
            &mut transaction,
            workspace_id,
            namespace,
        )
        .await?
        .ok_or(coral_provider::Error::ProviderNotFound)?;
        let keys: BTreeMap<&str, &ToolKey> = paths
            .iter()
            .map(|path| (path.relative_path(), path.key()))
            .collect();
        let mut updates = Vec::with_capacity(states.len());
        let mut missing = Vec::new();
        for (path, access_state) in states {
            match keys.get(path.as_str()) {
                Some(key) => updates.push((
                    path,
                    ToolPolicyTarget::new(workspace_id.clone(), provider.clone(), (*key).clone()),
                    *access_state,
                )),
                None => missing.push(path.clone()),
            }
        }
        if !missing.is_empty() {
            return Err(Error::ToolPathsNotFound(missing));
        }
        let mut applied = BTreeMap::new();
        for (path, target, access_state) in updates {
            let decision = write_access_state_in_transaction(
                &mut transaction,
                context,
                &evidence,
                target,
                access_state,
            )
            .await?;
            applied.insert(path.clone(), decision.access_state());
        }
        transaction.commit().await?;
        Ok(applied)
    }

    /// Lists safe Tool-policy Administrative Lifecycle Events.
    ///
    /// # Errors
    ///
    /// Requires current Workspace Owner authority.
    pub async fn list_lifecycle_events(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &coral_workspace::WorkspaceId,
    ) -> Result<Vec<AdministrativeLifecycleEvent>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let evidence = self
            .workspaces
            .check_in_transaction(
                &mut transaction,
                actor,
                workspace_id,
                WorkspaceAction::Inspect,
            )
            .await?;
        if evidence.effective_role() != WorkspaceRole::Owner {
            return Err(coral_workspace::Error::PermissionDenied(
                "Workspace Owner authority is required",
            )
            .into());
        }
        let events =
            repository::list_lifecycle_events_in_transaction(&mut transaction, workspace_id)
                .await?;
        transaction.rollback().await?;
        Ok(events)
    }
}

/// Writes one exact rule and its lifecycle Event, unless the rule already
/// holds `access_state`.
async fn write_access_state_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    context: &ChangeContext,
    evidence: &WorkspaceEvidence,
    target: ToolPolicyTarget,
    access_state: ToolAccessState,
) -> Result<ToolPolicyDecision, Error> {
    if repository::access_state_in_transaction(transaction, &target, coral_db::RowLock::None)
        .await?
        != Some(access_state)
    {
        repository::upsert_access_state(
            transaction,
            &target,
            access_state,
            context.occurred_at_unix_nanos(),
        )
        .await?;
        repository::insert_lifecycle_event(
            transaction,
            repository::NewLifecycleEvent {
                target: &target,
                actor_user_id: context.actor().user_id(),
                authority_source: evidence.authority_source(),
                changed_field_paths: &["access_state"],
                occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
                trace_id: context.trace_id(),
            },
        )
        .await?;
    }
    Ok(ToolPolicyDecision::new(target, access_state, true))
}

async fn require_tool_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    target: &ToolPolicyTarget,
) -> Result<(), Error> {
    coral_provider::tool_hints_and_method_in_transaction(
        transaction,
        target.workspace_id(),
        target.provider_id(),
        target.tool_key(),
    )
    .await?
    .map(|_| ())
    .ok_or(Error::ToolNotFound)
}

/// Reads one exact current Tool decision without holding it.
///
/// The caller owns a coherent read transaction and supplies declared hints and
/// the HTTP method from current structurally available Provider authoring
/// material. This facet does not lock or write policy and does not
/// independently resolve Provider persistence.
///
/// # Errors
///
/// Returns a corrupt-data or persistence failure.
pub async fn discovery_decision_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    target: &ToolPolicyTarget,
    current_hints: ToolHints,
    current_http_method: Option<HttpMethod>,
) -> Result<ToolPolicyDecision, Error> {
    let explicit =
        repository::access_state_in_transaction(transaction, target, coral_db::RowLock::None)
            .await?;
    Ok(resolved_decision(
        target,
        current_hints,
        current_http_method,
        explicit,
    ))
}

/// Reads every exact Tool rule of one Workspace for Universal Search.
///
/// Discovery reads the rules once per request instead of one decision per
/// Tool. The caller owns a coherent read transaction. It resolves each
/// Tool with [`ExactToolRules::access_state`], passing the declared
/// hints and HTTP method from current structurally available Provider
/// authoring material. This facet does not lock or write policy.
///
/// # Errors
///
/// Returns a corrupt-data or persistence failure.
pub async fn exact_tool_rules_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<ExactToolRules, Error> {
    repository::exact_rules_in_transaction(transaction, workspace_id).await
}

/// Rechecks one exact Tool policy decision for Invocation work, holding
/// the explicit rule as `lock` says.
///
/// The caller owns the transaction that either admits an Invocation or runs
/// its Dispatch Fence and supplies declared hints and the HTTP method from the
/// exact retained Provider Generation. A shared hold on the explicit rule
/// lets concurrent decisions read side by side while a policy change, which
/// writes the rule, waits for them, on both supported persistence Adapters;
/// a read snapshot holds nothing and only decides whether a held check is
/// needed.
///
/// # Errors
///
/// Returns an absent-target, corrupt-data, or persistence failure.
pub async fn invocation_decision_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    target: &ToolPolicyTarget,
    retained_hints: ToolHints,
    retained_http_method: Option<HttpMethod>,
    lock: coral_db::RowLock,
) -> Result<ToolPolicyDecision, Error> {
    let explicit = repository::access_state_in_transaction(transaction, target, lock).await?;
    Ok(resolved_decision(
        target,
        retained_hints,
        retained_http_method,
        explicit,
    ))
}

fn resolved_decision(
    target: &ToolPolicyTarget,
    hints: ToolHints,
    http_method: Option<HttpMethod>,
    explicit: Option<ToolAccessState>,
) -> ToolPolicyDecision {
    ToolPolicyDecision::new(
        target.clone(),
        explicit.unwrap_or_else(|| default_access_state(hints, http_method)),
        explicit.is_some(),
    )
}

/// A Tool runs without approval when it declares it changes nothing or
/// its HTTP method is safe. An undeclared hint is not evidence, so any other
/// Tool takes the cautious default.
pub(crate) fn default_access_state(
    hints: ToolHints,
    http_method: Option<HttpMethod>,
) -> ToolAccessState {
    if hints.declares_read_only() || http_method.is_some_and(HttpMethod::is_safe) {
        ToolAccessState::Enabled
    } else {
        ToolAccessState::ApprovalRequired
    }
}

#[cfg(test)]
mod tests;
