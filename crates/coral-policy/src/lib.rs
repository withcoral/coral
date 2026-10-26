//! Exact Workspace Operation policy for Coral.
//!
//! This private domain Module is the transport-neutral Interface for deciding
//! and changing one Operation Access State addressed by an exact Workspace,
//! Provider, and Operation Key. It supports only `enabled`,
//! `approval_required`, and `disabled`; no Principal, Account, Credential
//! Route, ordered, pattern, or glob dimensions enter the Interface.
//!
//! # Interface
//!
//! - [`OperationPolicyModule::decision`] returns the current exact decision.
//! - [`OperationPolicyModule::decisions`] reads a Provider's bounded selection in one snapshot.
//! - [`OperationPolicyModule::provider_rules`] reads one Provider's exact rules
//!   by Call Path for any Principal that inspects the Workspace.
//! - [`OperationPolicyModule::set_access_state`] creates or changes one exact
//!   rule for a Workspace Owner.
//! - [`OperationPolicyModule::set_access_states`] atomically changes 1–100 distinct
//!   exact decisions in one Workspace with their lifecycle evidence.
//! - [`OperationPolicyModule::set_provider_access_states`] atomically changes
//!   1–100 exact decisions of one Provider, each named by its Call Path.
//! - [`discovery_decision_in_transaction`] reads one exact current decision
//!   without a lock or a policy write, as when Run inspects an approval.
//! - [`exact_operation_rules_in_transaction`] reads every exact rule of one
//!   Workspace, so discovery decides all its Operations from one read.
//! - [`invocation_decision_in_transaction`] rechecks an exact retained decision
//!   inside Run-owned admission and Dispatch Fence transactions, holding the
//!   explicit rule shared when asked to.
//! - [`OperationPolicyModule::list_lifecycle_events`] exposes safe policy-domain
//!   Administrative Lifecycle Events.
//!
//! # Invariants
//!
//! The current active Provider catalog proves that the exact target exists.
//! Without an exact rule, a declared read-only Operation and an HTTP Operation
//! with a safe method are enabled; every other Operation is approval-required.
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

use coral_catalog::{HttpMethod, OperationHints, OperationKey};
use coral_db::Database;
use coral_provider::ProviderNamespace;
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, ChangeContext, WorkspaceAction, WorkspaceEvidence,
    WorkspaceId, WorkspaceModule, WorkspaceRole,
};

pub use error::Error;
pub use types::{
    AdministrativeLifecycleEvent, ExactOperationRules, LifecycleAction, LifecycleOutcome,
    OperationAccessState, OperationPolicyDecision, OperationPolicyTarget,
};

/// Exact Workspace Operation-policy Module.
#[derive(Clone)]
pub struct OperationPolicyModule {
    database: Arc<Database>,
    workspaces: WorkspaceModule,
}

impl OperationPolicyModule {
    /// Builds the Module from initialized persistence and Workspace authority.
    #[must_use]
    pub const fn new(database: Arc<Database>, workspaces: WorkspaceModule) -> Self {
        Self {
            database,
            workspaces,
        }
    }

    /// Returns the effective decision for one exact current Operation target.
    ///
    /// # Errors
    ///
    /// Returns an authority, absent-target, corrupt-data, or persistence error.
    pub async fn decision(
        &self,
        actor: &AuthenticatedUser,
        target: &OperationPolicyTarget,
    ) -> Result<OperationPolicyDecision, Error> {
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
        targets: &[OperationPolicyTarget],
    ) -> Result<Vec<OperationPolicyDecision>, Error> {
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
            .map(|target| target.operation_key().clone())
            .collect();
        let hints_and_methods = coral_provider::operation_hints_and_methods_batch_in_transaction(
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
                    .get(target.operation_key())
                    .ok_or(Error::OperationNotFound)?;
                Ok(resolved_decision(
                    target,
                    *hints,
                    *http_method,
                    explicit.get(target.operation_key()).copied(),
                ))
            })
            .collect::<Result<_, Error>>()?;
        transaction.rollback().await?;
        Ok(decisions)
    }

    /// Returns the exact rules a Workspace Owner set for the Provider under
    /// `coral.providers.<namespace>`, in any state, keyed by each Operation's
    /// Call Path relative to that Call Root. An Operation without a rule takes
    /// its default and is left out, and so is a rule whose Operation left the
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
    ) -> Result<BTreeMap<String, OperationAccessState>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.workspaces
            .check_principal_inspection_in_snapshot(
                &mut transaction,
                actor,
                workspace_id,
                now_unix_nanos,
            )
            .await?;
        let (provider, paths) = coral_provider::provider_operation_paths_in_transaction(
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

    /// Creates or changes one exact Operation Access State.
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
        target: OperationPolicyTarget,
        access_state: OperationAccessState,
    ) -> Result<OperationPolicyDecision, Error> {
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
        updates: Vec<(OperationPolicyTarget, OperationAccessState)>,
    ) -> Result<Vec<OperationPolicyDecision>, Error> {
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
            require_operation_in_transaction(&mut transaction, &target).await?;
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

    /// Atomically sets the exact state of 1–100 Operations of the Provider
    /// under `coral.providers.<namespace>`, in any state, each named by its
    /// Call Path relative to that Call Root. Each path resolves to its
    /// Operation in the transaction that writes its rule, so an update cannot
    /// give the state to another Operation. A rejected path leaves every
    /// decision and lifecycle Event unchanged, and a repeated state writes
    /// nothing. Returns each path's state.
    ///
    /// # Errors
    ///
    /// Rejects an empty or oversized batch, lost Owner authority, an absent
    /// Provider, and every path that names no Operation of its current
    /// catalog.
    pub async fn set_provider_access_states(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        namespace: &ProviderNamespace,
        states: &BTreeMap<String, OperationAccessState>,
    ) -> Result<BTreeMap<String, OperationAccessState>, Error> {
        if states.is_empty() || states.len() > 100 {
            return Err(Error::InvalidBatch);
        }
        let mut transaction = self.database.begin().await?;
        // The Workspace hold this takes serializes the change with every
        // Provider update in the Workspace, which takes it too, so each path
        // names one Operation until the commit.
        let evidence = self
            .workspaces
            .check_configuration_in_transaction(&mut transaction, context.actor(), workspace_id)
            .await?;
        let (provider, paths) = coral_provider::provider_operation_paths_in_transaction(
            &mut transaction,
            workspace_id,
            namespace,
        )
        .await?
        .ok_or(coral_provider::Error::ProviderNotFound)?;
        let keys: BTreeMap<&str, &OperationKey> = paths
            .iter()
            .map(|path| (path.relative_path(), path.key()))
            .collect();
        let mut updates = Vec::with_capacity(states.len());
        let mut missing = Vec::new();
        for (path, access_state) in states {
            match keys.get(path.as_str()) {
                Some(key) => updates.push((
                    path,
                    OperationPolicyTarget::new(
                        workspace_id.clone(),
                        provider.clone(),
                        (*key).clone(),
                    ),
                    *access_state,
                )),
                None => missing.push(path.clone()),
            }
        }
        if !missing.is_empty() {
            return Err(Error::OperationPathsNotFound(missing));
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

    /// Lists safe Operation-policy Administrative Lifecycle Events.
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
    target: OperationPolicyTarget,
    access_state: OperationAccessState,
) -> Result<OperationPolicyDecision, Error> {
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
    Ok(OperationPolicyDecision::new(target, access_state, true))
}

async fn require_operation_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    target: &OperationPolicyTarget,
) -> Result<(), Error> {
    coral_provider::operation_hints_and_method_in_transaction(
        transaction,
        target.workspace_id(),
        target.provider_id(),
        target.operation_key(),
    )
    .await?
    .map(|_| ())
    .ok_or(Error::OperationNotFound)
}

/// Reads one exact current Operation decision without holding it.
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
    target: &OperationPolicyTarget,
    current_hints: OperationHints,
    current_http_method: Option<HttpMethod>,
) -> Result<OperationPolicyDecision, Error> {
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

/// Reads every exact Operation rule of one Workspace for Universal Search.
///
/// Discovery reads the rules once per request instead of one decision per
/// Operation. The caller owns a coherent read transaction. It resolves each
/// Operation with [`ExactOperationRules::access_state`], passing the declared
/// hints and HTTP method from current structurally available Provider
/// authoring material. This facet does not lock or write policy.
///
/// # Errors
///
/// Returns a corrupt-data or persistence failure.
pub async fn exact_operation_rules_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<ExactOperationRules, Error> {
    repository::exact_rules_in_transaction(transaction, workspace_id).await
}

/// Rechecks one exact Operation policy decision for Invocation work, holding
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
    target: &OperationPolicyTarget,
    retained_hints: OperationHints,
    retained_http_method: Option<HttpMethod>,
    lock: coral_db::RowLock,
) -> Result<OperationPolicyDecision, Error> {
    let explicit = repository::access_state_in_transaction(transaction, target, lock).await?;
    Ok(resolved_decision(
        target,
        retained_hints,
        retained_http_method,
        explicit,
    ))
}

fn resolved_decision(
    target: &OperationPolicyTarget,
    hints: OperationHints,
    http_method: Option<HttpMethod>,
    explicit: Option<OperationAccessState>,
) -> OperationPolicyDecision {
    OperationPolicyDecision::new(
        target.clone(),
        explicit.unwrap_or_else(|| default_access_state(hints, http_method)),
        explicit.is_some(),
    )
}

/// An Operation runs without approval when it declares it changes nothing or
/// its HTTP method is safe. An undeclared hint is not evidence, so any other
/// Operation takes the cautious default.
pub(crate) fn default_access_state(
    hints: OperationHints,
    http_method: Option<HttpMethod>,
) -> OperationAccessState {
    if hints.declares_read_only() || http_method.is_some_and(HttpMethod::is_safe) {
        OperationAccessState::Enabled
    } else {
        OperationAccessState::ApprovalRequired
    }
}

#[cfg(test)]
mod tests;
