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
//!   inside Run-owned admission and Policy Enforcement Point transactions,
//!   holding the explicit rule shared when asked to.
//! - [`ToolPolicyModule::list_lifecycle_events`] exposes safe policy-domain
//!   Administrative Lifecycle Events.
//! - [`remove_authored_tool_rules_in_transaction`] removes the exact rules
//!   of removed Private and Shared Tools inside the transaction that removes
//!   them.
//!
//! # Invariants
//!
//! The current active Provider catalog proves that the exact target exists.
//! The Private and the Shared Provider keep no catalog: a current Private
//! Tool the reader can read, or a live Shared Tool, proves it instead.
//! Without an exact rule, a declared read-only Tool, an HTTP Tool with a
//! safe method and a Tool its catalog declares `enabled` are enabled, a
//! Private or Shared Tool takes the default its current revision declares,
//! and every other Tool is approval-required.
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

use coral_catalog::{HttpMethod, ToolKey, ToolPolicyFacts};
use coral_db::Database;
use coral_provider::{ProviderCallRoot, ProviderNamespace};
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, ChangeContext, WorkspaceAction, WorkspaceEvidence,
    WorkspaceId, WorkspaceModule, WorkspaceRole,
};

pub use coral_catalog::ToolAccessState;
pub use error::Error;
pub use types::{
    AdministrativeLifecycleEvent, ExactToolRules, LifecycleAction, LifecycleOutcome, ListedTool,
    PrivateToolOwner, ToolPolicyDecision, ToolPolicyTarget,
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
    /// In the Private Provider, `actor` reads only the Private Tools it can
    /// read: its own, and an Owner's every Agent's. A Member's own takes its
    /// declared default. In the Shared Provider, every reader reads the
    /// Workspace's live Shared Tools and their rules. A Private or Shared
    /// Tool's default is its current revision's; a Run that linked an
    /// earlier revision keeps that one's.
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
        let evidence = self
            .workspaces
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
        let ruled = ruled_tool_facts_in_transaction(
            &mut transaction,
            actor,
            &evidence,
            first.provider_id(),
            &keys,
        )
        .await?;
        let explicit = if ruled.rules_apply {
            repository::access_states_in_transaction(
                &mut transaction,
                first.workspace_id(),
                first.provider_id(),
                &keys,
            )
            .await?
        } else {
            BTreeMap::new()
        };
        let decisions = targets
            .iter()
            .map(|target| {
                let facts = ruled
                    .facts
                    .get(target.tool_key())
                    .ok_or(Error::ToolNotFound)?;
                Ok(resolved_decision(
                    target,
                    *facts,
                    explicit.get(target.tool_key()).copied(),
                ))
            })
            .collect::<Result<_, Error>>()?;
        transaction.rollback().await?;
        Ok(decisions)
    }

    /// Reads one catalog page of a Provider with each listed Tool's current
    /// decision, in Tool Key order. It leaves out each Tool `actor` does not
    /// reach. Authority, the catalog and explicit rules share one read
    /// snapshot, and a full page carries one lookahead row. In the Private
    /// Provider the page holds the Private Tools whose rules `actor` may
    /// read: its own and, for an Owner, every Agent's, each naming its Agent.
    /// In the Shared Provider it holds the Workspace's live Shared Tools,
    /// the same for every reader.
    ///
    /// # Errors
    /// Rejects lost authority, a Provider `actor` may not inspect, an invalid
    /// page size, and persistence failures.
    pub async fn listed_decisions(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        provider: &coral_provider::ProviderId,
        after: Option<&ToolKey>,
        page_size: u32,
    ) -> Result<Vec<ListedTool>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let evidence = self
            .workspaces
            .check_in_transaction(&mut transaction, actor, workspace, WorkspaceAction::Inspect)
            .await?;
        let ListedPage {
            listings,
            rules_apply,
        } = listed_page_in_transaction(
            &mut transaction,
            actor,
            &evidence,
            provider,
            after,
            page_size,
        )
        .await?;
        let keys: Vec<_> = listings
            .iter()
            .map(|(tool, _agent)| tool.key.clone())
            .collect();
        let explicit = if keys.is_empty() || !rules_apply {
            BTreeMap::new()
        } else {
            repository::access_states_in_transaction(&mut transaction, workspace, provider, &keys)
                .await?
        };
        transaction.rollback().await?;
        Ok(listings
            .into_iter()
            .map(|(listing, agent)| {
                let target =
                    ToolPolicyTarget::new(workspace.clone(), provider.clone(), listing.key.clone());
                let decision =
                    resolved_decision(&target, listing.policy, explicit.get(&listing.key).copied());
                ListedTool {
                    listing,
                    decision,
                    agent,
                }
            })
            .collect())
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
            require_tool_in_transaction(&mut transaction, context.actor(), &evidence, &target)
                .await?;
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

/// One page of a Provider's Tools as a policy listing shows them.
struct ListedPage {
    /// Each Tool's row, with the Agent whose Private Tool it is.
    listings: Vec<(
        coral_catalog::ToolListing,
        Option<coral_workspace::PrincipalId>,
    )>,
    /// Whether an exact rule applies to them.
    rules_apply: bool,
}

/// One page of the Tools of `provider` whose rules `actor`, with `evidence`
/// in its Workspace, may read, in Tool Key order after `after`, with one
/// lookahead row.
async fn listed_page_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    actor: &AuthenticatedUser,
    evidence: &WorkspaceEvidence,
    provider: &coral_provider::ProviderId,
    after: Option<&ToolKey>,
    page_size: u32,
) -> Result<ListedPage, Error> {
    let workspace = evidence.workspace().id();
    Ok(
        match RuledSource::of(transaction, workspace, provider).await? {
            // This read checks the page size itself.
            RuledSource::Generation => ListedPage {
                listings: coral_provider::tool_listings_in_transaction(
                    transaction,
                    evidence,
                    provider,
                    after,
                    page_size,
                )
                .await?
                .into_iter()
                .map(|listing| (listing, None))
                .collect(),
                rules_apply: true,
            },
            RuledSource::PrivateTools => {
                let reach = PrivateToolReach::of(evidence);
                let reader = AuthenticatedPrincipal::user(actor.clone());
                let page = coral_tools::readable_page_in_transaction(
                    transaction,
                    workspace,
                    reader.principal(),
                    reach.scope,
                    after,
                    authored_tool_page_size(page_size)?,
                )
                .await
                .map_err(tools_error)?;
                ListedPage {
                    listings: page
                        .iter()
                        .map(|tool| {
                            let listing = authored_tool_listing(
                                tool.key(),
                                tool.path().as_str(),
                                tool.default_access_state(),
                            );
                            (listing, tool.agent().cloned())
                        })
                        .collect(),
                    rules_apply: reach.rules_apply,
                }
            }
            RuledSource::SharedTools => {
                let page = coral_tools::shared_tool_default_page_in_transaction(
                    transaction,
                    workspace,
                    after,
                    authored_tool_page_size(page_size)?,
                )
                .await
                .map_err(tools_error)?;
                ListedPage {
                    listings: page
                        .iter()
                        .map(|tool| {
                            let listing = authored_tool_listing(
                                tool.key(),
                                tool.path().as_str(),
                                tool.default_access_state(),
                            );
                            (listing, None)
                        })
                        .collect(),
                    rules_apply: true,
                }
            }
        },
    )
}

/// `page_size`, when a page of Private or Shared Tools may hold that many.
fn authored_tool_page_size(page_size: u32) -> Result<u32, Error> {
    if (1..=coral_catalog::MAX_TOOL_LISTING_PAGE).contains(&page_size) {
        Ok(page_size)
    } else {
        Err(coral_provider::Error::InvalidInput("Tool page size is out of range".to_owned()).into())
    }
}

/// A Private or Shared Tool's row, as a Provider Tool's would be:
/// available, with no tags or diagnostics, and `default`, the default of
/// its current revision, as its policy facts.
fn authored_tool_listing(
    key: &ToolKey,
    relative_path: &str,
    default: ToolAccessState,
) -> coral_catalog::ToolListing {
    coral_catalog::ToolListing {
        key: key.clone(),
        relative_path: relative_path.to_owned(),
        tags: Vec::new(),
        policy: declared_default_facts(default),
        availability: coral_catalog::ToolAvailability::Available,
        diagnostics: Vec::new(),
    }
}

/// The policy facts of a Private or Shared Tool: only the default its
/// current revision declares.
fn declared_default_facts(default: ToolAccessState) -> ToolPolicyFacts {
    ToolPolicyFacts::default().declaring_default(Some(default))
}

/// A coral-tools read failure: only persistence or corrupt data.
fn tools_error(error: coral_tools::Error) -> Error {
    match error {
        coral_tools::Error::Database(error) => Error::Database(error),
        other => Error::CorruptData(other.to_string()),
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
    actor: &AuthenticatedUser,
    evidence: &WorkspaceEvidence,
    target: &ToolPolicyTarget,
) -> Result<(), Error> {
    ruled_tool_facts_in_transaction(
        transaction,
        actor,
        evidence,
        target.provider_id(),
        std::slice::from_ref(target.tool_key()),
    )
    .await?
    .facts
    .contains_key(target.tool_key())
    .then_some(())
    .ok_or(Error::ToolNotFound)
}

/// The Tools of one Provider that a reader may read or set a rule on, with
/// their policy facts, and whether an exact rule applies to them.
struct RuledTools {
    facts: BTreeMap<ToolKey, ToolPolicyFacts>,
    rules_apply: bool,
}

/// The policy facts of each of `keys` that names a Tool of `provider` on
/// which `actor`, with `evidence` in its Workspace, may read or set a rule:
/// a Tool of the Provider's current catalog, a live Shared Tool in the
/// Workspace's Shared Provider or, in its Private Provider, a Private Tool
/// `actor` can read. That is its own and, for an Owner, every Agent's,
/// never another User's (ADR 0033). A Private or Shared Tool's facts are
/// the default its current revision declares. A rule applies to a Private
/// Tool only while `actor` is an Owner: no rule applies to a Member's. Keys
/// that name no such Tool are left out.
async fn ruled_tool_facts_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    actor: &AuthenticatedUser,
    evidence: &WorkspaceEvidence,
    provider: &coral_provider::ProviderId,
    keys: &[ToolKey],
) -> Result<RuledTools, Error> {
    let workspace_id = evidence.workspace().id();
    match RuledSource::of(transaction, workspace_id, provider).await? {
        RuledSource::Generation => Ok(RuledTools {
            facts: coral_provider::tool_policy_facts_batch_in_transaction(
                transaction,
                workspace_id,
                provider,
                keys,
            )
            .await?,
            rules_apply: true,
        }),
        RuledSource::PrivateTools => {
            let reach = PrivateToolReach::of(evidence);
            let reader = AuthenticatedPrincipal::user(actor.clone());
            let facts = coral_tools::readable_defaults_in_transaction(
                transaction,
                workspace_id,
                reader.principal(),
                reach.scope,
                keys,
            )
            .await
            .map_err(tools_error)?
            .into_iter()
            .map(|(key, declared)| (key, declared_default_facts(declared)))
            .collect();
            Ok(RuledTools {
                facts,
                rules_apply: reach.rules_apply,
            })
        }
        RuledSource::SharedTools => Ok(RuledTools {
            facts: coral_tools::shared_tool_defaults_in_transaction(
                transaction,
                workspace_id,
                keys,
            )
            .await
            .map_err(tools_error)?
            .into_iter()
            .map(|(key, declared)| (key, declared_default_facts(declared)))
            .collect(),
            rules_apply: true,
        }),
    }
}

/// Where the Tools that take a Provider's rules are kept.
enum RuledSource {
    /// The Provider's current Generation: its catalog.
    Generation,
    /// The Private Tools that coral-tools keeps, each Principal's own.
    PrivateTools,
    /// The Workspace's live Shared Tools, which coral-tools keeps. Every
    /// Member User reads them and their rules, and a rule on one applies to
    /// every caller.
    SharedTools,
}

impl RuledSource {
    /// The source of `provider`'s Tools in the Workspace, by the kind of
    /// Provider its Call Root names. A new kind must be decided here.
    async fn of(
        transaction: &mut coral_db::Transaction<'_>,
        workspace_id: &WorkspaceId,
        provider: &coral_provider::ProviderId,
    ) -> Result<Self, Error> {
        let call_root =
            coral_provider::provider_call_root_in_transaction(transaction, workspace_id, provider)
                .await?;
        Ok(match call_root {
            Some(ProviderCallRoot::Private) => Self::PrivateTools,
            Some(ProviderCallRoot::Shared) => Self::SharedTools,
            // An Owner-added Provider and the Built-in Provider keep their
            // Tools in their Generation. An absent Provider answers as a
            // Generation read does.
            Some(ProviderCallRoot::Owner(_) | ProviderCallRoot::Builtin) | None => Self::Generation,
        })
    }
}

/// What a reader reads of the Private Provider (ADR 0033).
struct PrivateToolReach {
    /// The Private Tools whose policy it reads.
    scope: coral_tools::PrivateToolScope,
    /// Whether an Owner's exact rule applies to them.
    rules_apply: bool,
}

impl PrivateToolReach {
    /// An Owner reads its own Private Tools and every Agent's, and its
    /// rules apply to them. Anyone else reads only its own, which take
    /// their declared default: no rule applies to a Member's.
    fn of(evidence: &WorkspaceEvidence) -> Self {
        if evidence.effective_role() == WorkspaceRole::Owner {
            Self {
                scope: coral_tools::PrivateToolScope::OwnAndAgents,
                rules_apply: true,
            }
        } else {
            Self {
                scope: coral_tools::PrivateToolScope::Own,
                rules_apply: false,
            }
        }
    }
}

/// Reads one exact current Tool decision without holding it.
///
/// The caller owns a coherent read transaction and supplies the policy facts
/// from current structurally available Provider authoring material. This
/// facet does not lock or write policy and does not independently resolve
/// Provider persistence.
///
/// # Errors
///
/// Returns a corrupt-data or persistence failure.
pub async fn discovery_decision_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    target: &ToolPolicyTarget,
    current: ToolPolicyFacts,
) -> Result<ToolPolicyDecision, Error> {
    let explicit =
        repository::access_state_in_transaction(transaction, target, coral_db::RowLock::None)
            .await?;
    Ok(resolved_decision(target, current, explicit))
}

/// Reads every exact Tool rule of one Workspace for Universal Search.
///
/// Discovery reads the rules once per request instead of one decision per
/// Tool. The caller owns a coherent read transaction. It resolves each
/// Tool with [`ExactToolRules::access_state`], passing the policy facts
/// from current structurally available Provider authoring material. This facet does not lock or write policy.
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
/// its Policy Enforcement Point and supplies the policy facts from the exact
/// retained Provider Generation. A shared hold on the explicit
/// rule lets concurrent decisions read side by side while a policy change,
/// which writes the rule, waits for them, on both supported persistence
/// Adapters; a read snapshot holds nothing and only decides whether a held
/// check is needed.
///
/// # Errors
///
/// Returns an absent-target, corrupt-data, or persistence failure.
pub async fn invocation_decision_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    target: &ToolPolicyTarget,
    retained: ToolPolicyFacts,
    lock: coral_db::RowLock,
) -> Result<ToolPolicyDecision, Error> {
    let explicit = repository::access_state_in_transaction(transaction, target, lock).await?;
    Ok(resolved_decision(target, retained, explicit))
}

/// Rechecks the decision for one Invocation of a Private or Shared Tool,
/// as [`invocation_decision_in_transaction`] does for a Provider Tool: the
/// Owner's exact rule when it applies, held as `lock` says, else
/// `declared`, the default of the revision the Run linked.
///
/// A Shared Tool is the Workspace's, so a rule on it applies to every
/// caller. A Private Tool is its `caller`'s own, so a rule applies only
/// while the caller is an Owner or an Agent: a rule on a Member's Private
/// Tool is not read.
///
/// # Errors
///
/// Returns a corrupt-data or persistence failure.
pub async fn authored_tool_decision_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    target: &ToolPolicyTarget,
    kind: coral_catalog::AuthoredToolKind,
    declared: ToolAccessState,
    caller: PrivateToolOwner,
    lock: coral_db::RowLock,
) -> Result<ToolPolicyDecision, Error> {
    let explicit = if caller.takes_rule_on(kind) {
        repository::access_state_in_transaction(transaction, target, lock).await?
    } else {
        None
    };
    Ok(ToolPolicyDecision::new(
        target.clone(),
        explicit.unwrap_or(declared),
        explicit.is_some(),
    ))
}

/// Removes the exact rules on the Tool Keys `removed` of the Workspace's
/// Provider for Tools of `kind`: the rules that name the Private or the
/// Shared Tools that the caller removes in this transaction. A Tool's
/// identity is its Tool Key there. A Tool without a rule has none to remove.
///
/// The caller owns the removal, its authority and its transaction, and
/// commits the two together. This facet records no policy Event: the Tool's
/// own removal Event is the record, as when a Provider's removal drops its
/// rules.
///
/// # Errors
///
/// Returns [`Error::Database`] for a persistence failure and
/// [`Error::CorruptData`] for a corrupt Provider, and no other error.
pub async fn remove_authored_tool_rules_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
    kind: coral_catalog::AuthoredToolKind,
    removed: &[ToolKey],
) -> Result<(), Error> {
    if removed.is_empty() {
        return Ok(());
    }
    // A rule names its Provider, so a Workspace without the kind's Provider
    // has no rule on a Tool of that kind.
    let Some(provider) =
        coral_provider::authored_tool_provider_in_transaction(transaction, workspace_id, kind)
            .await
            .map_err(provider_read_error)?
    else {
        return Ok(());
    };
    repository::delete_access_states(transaction, workspace_id, provider.id(), removed).await
}

/// A coral-provider read failure: only persistence or corrupt data.
fn provider_read_error(error: coral_provider::Error) -> Error {
    match error {
        coral_provider::Error::Database(error)
        | coral_provider::Error::Catalog(coral_catalog::Error::Database(error))
        | coral_provider::Error::Workspace(coral_workspace::Error::Database(error)) => {
            Error::Database(error)
        }
        other => Error::CorruptData(other.to_string()),
    }
}

fn resolved_decision(
    target: &ToolPolicyTarget,
    facts: ToolPolicyFacts,
    explicit: Option<ToolAccessState>,
) -> ToolPolicyDecision {
    ToolPolicyDecision::new(
        target.clone(),
        explicit.unwrap_or_else(|| default_access_state(facts)),
        explicit.is_some(),
    )
}

/// Whether a Tool only reads provider state: it declares it changes
/// nothing, or its HTTP method is safe. An undeclared hint is not evidence,
/// and a default the catalog declares says nothing about effects.
#[must_use]
pub fn reads_only(facts: ToolPolicyFacts) -> bool {
    facts.hints().declares_read_only() || facts.http_method().is_some_and(HttpMethod::is_safe)
}

/// A Tool takes the state its catalog or author declares: Coral's own
/// catalog declares `enabled` for the Tools that change only the caller's
/// own state (ADR 0031), and a Private Tool's author declares any state
/// (ADR 0033). Otherwise a Tool that only reads runs without approval, and
/// any other Tool takes the cautious default.
pub(crate) fn default_access_state(facts: ToolPolicyFacts) -> ToolAccessState {
    match facts.declared_default() {
        Some(declared) => declared,
        None if reads_only(facts) => ToolAccessState::Enabled,
        None => ToolAccessState::ApprovalRequired,
    }
}

#[cfg(test)]
mod tests;
