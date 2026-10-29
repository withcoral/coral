//! The read side of Universal Search: one [`EntityRetriever`] per Search
//! Entity kind, the sibling of the indexer that writes the same rows.
//!
//! A retriever applies its owners' visibility rules once per request, puts
//! the answer into its ranked statement over the search-owned table, and
//! hands the engine positions only. It hydrates and describes the Entities
//! of its kind. Every method joins the caller's read snapshot and opens no
//! connection of its own.

pub(crate) mod functions;
pub(crate) mod plan;
mod repository;
pub(crate) mod tools;

pub(crate) use repository::{abandon_query, begin_query, end_query};

use std::sync::Arc;

use async_trait::async_trait;
use coral_db::Transaction;
use coral_functions::SavedFunctionSearchMaterial;
use coral_policy::ExactToolRules;
use coral_provider::{ActiveProvider, LoadedGenerations};
use coral_workspace::{AuthenticatedPrincipal, PrincipalWorkspaceEvidence, WorkspaceId};

use crate::{
    CallPath, EntityKind, Error, ExactAuthoringDescription, ProviderCallRoot, RetrieverCoverage,
    SearchHit,
};

/// The request one retriever call serves: who asks, in which Workspace, the
/// engine's normalized query terms, if any, and the Generations its hits
/// have loaded.
#[derive(Clone, Copy)]
pub(crate) struct RequestContext<'a> {
    pub(crate) actor: &'a AuthenticatedPrincipal,
    /// The caller's Workspace authority, checked once for this request.
    pub(crate) authority: &'a PrincipalWorkspaceEvidence,
    pub(crate) workspace_id: &'a WorkspaceId,
    pub(crate) terms: Option<&'a [String]>,
    pub(crate) loaded: &'a LoadedGenerations,
}

/// One ranked or listed Search Entity. The payload is the hydration handle
/// of the retriever that produced it; the engine reads only the Call Path.
#[derive(Clone)]
pub(crate) enum EntityRef {
    Tool(ToolRef),
    SavedFunction(SavedFunctionRef),
}

/// A Tool of a Provider's active Generation, with what hydration needs:
/// the Provider as the visibility step read it, the Tool Key, the
/// Workspace's exact rules, loaded once per request, and the near-spellings
/// its ranking matched.
#[derive(Clone)]
pub(crate) struct ToolRef {
    provider: Arc<ActiveProvider>,
    tool_key: coral_catalog::ToolKey,
    rules: Arc<ExactToolRules>,
    call_path: CallPath,
    near_spellings: Arc<[String]>,
}

/// A Saved Function of the requesting Principal, with the near-spellings its
/// ranking matched.
#[derive(Clone)]
pub(crate) struct SavedFunctionRef {
    material: SavedFunctionSearchMaterial,
    call_path: CallPath,
    near_spellings: Arc<[String]>,
}

/// The Call Path of an entity a retriever ranked, as a span field.
pub(crate) struct RankedCallPath<'a>(pub(crate) &'a CallPath);

/// The search index holds only the Tools and Saved Functions Coral serves,
/// so a ranked entity's Call Path names one of them.
impl coral_telemetry::safe::Identifier for RankedCallPath<'_> {}

impl std::fmt::Display for RankedCallPath<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0.as_str())
    }
}

impl EntityRef {
    pub(crate) const fn call_path(&self) -> &CallPath {
        match self {
            Self::Tool(tool) => &tool.call_path,
            Self::SavedFunction(function) => &function.call_path,
        }
    }
}

/// Every Entity of one kind the Principal may see, and how many units they
/// come from: Providers for Tools, Saved Functions for functions.
pub(crate) struct Listing {
    pub(crate) entities: Vec<EntityRef>,
    pub(crate) units: usize,
}

/// What one retriever's `query` asks: the engine's normalized terms, the
/// Provider Call Root filter, and the window size.
pub(crate) struct QueryRequest<'a> {
    pub(crate) terms: &'a [String],
    pub(crate) provider_filter: Option<&'a ProviderCallRoot>,
    pub(crate) window: usize,
}

/// One retriever's answer to a query: the window in rank order and its
/// coverage. The state follows from the coverage.
pub(crate) struct Answer {
    pub(crate) window: Vec<EntityRef>,
    pub(crate) coverage: RetrieverCoverage,
}

/// Why a retriever could not answer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum QueryError {
    /// This retriever's ranked statement failed. The engine reports `error`
    /// for it and keeps the other retrievers' results.
    #[error("the retriever's ranked statement failed")]
    Retriever(#[source] RetrievalError),
    /// A shared read failed, such as an owner's visibility read, or the
    /// request names a Provider the caller cannot search. The request fails.
    #[error(transparent)]
    Shared(#[from] Error),
}

/// Why a retriever's own read of the search-owned tables failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RetrievalError {
    /// The statement failed.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
    /// A search-owned row holds a value its column does not admit.
    #[error("corrupt search index: {0}")]
    Corrupt(&'static str),
}

impl RetrievalError {
    /// The error's stable kind, for an event's `error.type`: a bounded
    /// code that never carries the error's text.
    pub(crate) const fn code(&self) -> &'static str {
        match self {
            Self::Database { .. } => "database",
            Self::Corrupt { .. } => "corrupt",
        }
    }
}

impl QueryError {
    /// A failed read of the search-owned tables: this retriever's error.
    pub(crate) fn retrieval(error: impl Into<RetrievalError>) -> Self {
        Self::Retriever(error.into())
    }
}

/// What hydrating one ranked or listed Entity gives.
pub(crate) enum Hydrated {
    /// The page item, with evidence for a query.
    Hit(Box<SearchHit>),
    /// The Entity leaves the page: it vanished, policy disables it now, or
    /// its current material no longer matches the query (the index row is
    /// stale). The page goes on.
    Dropped,
    /// The Entity's authoritative material cannot be read. The page goes on;
    /// a page whose every candidate is unreadable is unavailable.
    Unreadable,
}

/// Reads one Search Entity kind for Universal Search.
#[async_trait]
pub(crate) trait EntityRetriever: Send + Sync {
    /// The kind this retriever ranks, named on its status.
    fn kind(&self) -> EntityKind;

    /// Every Entity of this kind the Principal may see now that the Provider
    /// filter leaves, unordered. Reads no index.
    async fn list(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        provider_filter: Option<&ProviderCallRoot>,
    ) -> Result<Listing, Error>;

    /// Applies the owners' visibility rules, runs this kind's ranked
    /// statement over the search-owned table, and returns one ranked window.
    async fn query(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        request: &QueryRequest<'_>,
    ) -> Result<Answer, QueryError>;

    /// One page item from the entity's payload, in the same snapshot, with
    /// evidence for the context's terms.
    async fn hydrate(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        entity: &EntityRef,
    ) -> Result<Hydrated, Error>;

    /// The exact Authoring Description by Call Path. Reads no index. `None`
    /// for another kind's path, an absent path, and an invisible path alike.
    async fn describe(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        call_path: &CallPath,
    ) -> Result<Option<ExactAuthoringDescription>, Error>;
}

/// Counts for a coverage: `usize` values the host cannot exceed as `u64`.
pub(crate) fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
