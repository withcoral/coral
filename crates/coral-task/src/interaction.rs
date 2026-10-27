//! Task history types. Summaries never contain discovery responses or diagnostics.
use crate::{Error, TaskId, TaskIntent};
use coral_workspace::{AuthenticatedPrincipal, TraceId, WorkspaceId};
use std::fmt;
use uuid::Uuid;

/// Stable identity of one authorized attempt, generated before work.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct InteractionId(Uuid);
impl InteractionId {
    pub(crate) fn generate() -> Self {
        Self(Uuid::new_v4())
    }
    /// Parses a canonical identity.
    /// # Errors
    /// Rejects invalid or noncanonical UUIDs.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let id = Uuid::parse_str(value)
            .map_err(|_invalid_id| Error::InvalidInput("invalid interaction identity".into()))?;
        if id.to_string() != value {
            return Err(Error::InvalidInput("invalid interaction identity".into()));
        }
        Ok(Self(id))
    }
}
impl fmt::Display for InteractionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Authenticated attribution for one command, never passed to lower domains.
#[derive(Clone)]
pub struct AttributedCommand {
    pub(crate) actor: AuthenticatedPrincipal,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) task_id: TaskId,
    pub(crate) intent: TaskIntent,
    pub(crate) trace_id: TraceId,
}
impl AttributedCommand {
    /// Supplies exact authenticated scope and separate per-call intent.
    #[must_use]
    pub const fn new(
        actor: AuthenticatedPrincipal,
        workspace_id: WorkspaceId,
        task_id: TaskId,
        intent: TaskIntent,
        trace_id: TraceId,
    ) -> Self {
        Self {
            actor,
            workspace_id,
            task_id,
            intent,
            trace_id,
        }
    }
}

/// Tool attempted toward the goal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptKind {
    /// One requested search page.
    Search,
    /// One exact current description.
    Describe,
    /// Non-executing Saved Function save.
    Save,
    /// Current Saved Function deletion.
    Delete,
    /// Program admission, not its execution outcome.
    Submit,
}

/// Honest durable attempt status, independent of the goal or execution outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcome {
    /// No definite completion committed. Work must not be replayed automatically.
    Unknown,
    /// Complete discovery or owning mutation committed.
    Succeeded,
    /// The owning domain definitively rejected authorized work.
    Rejected,
}

/// Safe bounded rejection, selected by the operation owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptFailure {
    /// Stable failure category.
    pub code: String,
    /// Safe caller-visible explanation, including bounded authored diagnostics.
    pub message: String,
}

/// Task-owned accepted discovery request, independent of current callable definitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryContext {
    /// One requested Search page, including the effective accepted limit.
    Search {
        /// Exact accepted query; absent for catalog browsing.
        query: Option<String>,
        /// Exact accepted Provider identity; absent when unfiltered.
        provider: Option<String>,
        /// Effective accepted page size.
        limit: u16,
        /// Exact requested continuation; absent on the first page.
        cursor: Option<String>,
    },
    /// One exact callable description request.
    Describe {
        /// Exact accepted callable path, not a current-definition reference.
        call_path: String,
        /// Types of that path's declaration asked for, in the order given;
        /// empty for a plain description.
        types: Vec<String>,
    },
}

/// Exact-read-only evidence; owning contents are linked, except discovery snapshots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptEvidence {
    /// Complete accepted request context and immutable response projection.
    Discovery {
        /// Only accepted query/filter/page or Call Path context, never transport dumps.
        context: DiscoveryContext,
        /// The complete response returned by Search at that time.
        response: serde_json::Value,
    },
    /// Durable Run acceptance. Execution results belong to Run.
    Run(coral_run::RunId),
    /// Saved Function identity. Current definition and lifecycle evidence remain independent.
    SavedFunction(coral_functions::SavedFunctionId),
    /// Known rejection without an accepted owning mutation.
    Rejected(AttemptFailure),
}

impl AttemptEvidence {
    pub(crate) const fn outcome(&self) -> AttemptOutcome {
        match self {
            Self::Rejected(_) => AttemptOutcome::Rejected,
            Self::Discovery { .. } | Self::Run(_) | Self::SavedFunction(_) => {
                AttemptOutcome::Succeeded
            }
        }
    }
}

/// Bounded source-free summary of one attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InteractionSummary {
    /// Retained discovery response bytes. Other tool responses are not measured.
    /// Accepted execution impact belongs to the associated Run, never this attempt.
    pub context_impact_bytes: Option<i64>,
    /// Immutable owning Workspace.
    pub workspace: coral_workspace::WorkspaceId,
    /// Accountable participating Principal, not a credential owner or reviewer.
    pub principal: coral_workspace::Principal,
    /// Stable attempt identity.
    pub id: InteractionId,
    /// Attempt kind.
    pub kind: AttemptKind,
    /// Exact per-call intent, distinct from the goal intent.
    pub intent: TaskIntent,
    /// First durable completion or unresolved uncertainty.
    pub outcome: AttemptOutcome,
    /// Durable begin time.
    pub started_at_unix_nanos: i64,
    /// Durable completion time, if known.
    pub completed_at_unix_nanos: Option<i64>,
    /// Accepted Run reference, without loading exact evidence.
    pub run: Option<coral_run::RunId>,
}

/// Authorized exact historical detail, never reconstructed from current definitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InteractionDetail {
    /// Source-free metadata.
    pub summary: InteractionSummary,
    /// Complete retained evidence, or unknown completion.
    pub evidence: Option<AttemptEvidence>,
}
