//! Principal-filtered TypeScript Universal Search for Coral.
//!
//! This private domain Module is the transport-neutral Interface for searching
//! current Search Entities, which returns Search Hits, and describing one exact
//! Call Path, which returns its Authoring Description with the declaration
//! abridged, or the types of it that the caller names.
//! It owns its index end to end: the committing workflows notify it, it
//! writes a search-owned table, and it ranks inside the database. It composes
//! Provider readiness, exact Tool policy, Principal-owned Saved
//! Functions, and Workspace authority, and stores none of them. It has no
//! upstream sender, Program Run data input, or observed-value input.
//!
//! # Interface
//!
//! - [`UniversalSearchModule::discover`] is the one discovery operation, which
//!   the Built-in Tool `coral.workspace.search` reaches, each request in one
//!   read snapshot. A query runs one ranked statement per Search Entity kind,
//!   one retriever each, and fuses their windows by position, strong matches
//!   first, into bounded pages of Search Hits with one status per retriever.
//!   An omitted query lists current items in Call Path order and reads no
//!   index. Exact Call Paths resolve to their Authoring
//!   Descriptions and read no index; `signature::abridge` shortens the
//!   declaration each returns, while search reads the complete one. A path
//!   that names nothing the caller can discover comes back missing, with
//!   the nearest Discoverable Call Paths of its Call Root.
//!   [`UniversalSearchModule::search`] and [`UniversalSearchModule::describe`]
//!   are its query mode and its one-path mode.
//! - [`projection`] turns what discovery found into the result types
//!   coral-catalog declares for `coral.workspace.search`, the shape the
//!   `search` and `describe` records in Task history retain.
//! - [`UniversalSearchModule::searchable_providers`] lists the Provider
//!   Call Roots a search's filter accepts from the caller. A filter that names
//!   another is refused with that list.
//! - [`UniversalSearchModule::inspect_provider_readiness`] derives caller readiness
//!   from the Tool eligibility rule discovery uses, without reading Saved
//!   Functions or the index.
//! - [`UniversalSearchModule::inspect_search_index_states`] reports whether
//!   the index holds each Provider's active Generation, apart from readiness.
//! - [`prepare_storage`] runs at boot before `Database::migrate` applies
//!   `coral_db::migration_plan()`, which creates the search index's tables. It
//!   checks the database features their text search needs.
//! - [`SearchIndex`] writes the search-owned index in the background when a
//!   committing workflow notifies it, and its reconcile task converges the
//!   index on what the owners hold.
//!
//! The engine inside [`UniversalSearchModule`] normalizes the query, hands the
//! same terms to every retriever, fuses by position, and pages by a plain
//! offset that authorizes nothing. It never names a Search Entity kind. Every search
//! page and exact description applies the owners' current visibility rules.
//! Discovery never starts an interactive authorization or refreshes credentials.

mod discovery;
mod eligibility;
mod error;
mod hit;
mod index;
mod index_state;
mod indexer;
mod page;
pub mod projection;
mod readiness;
mod retriever;
mod signature;
mod snippet;
mod storage;
mod suggestion;
#[cfg(any(test, feature = "test-fixtures"))]
pub mod test_fixtures;
mod types;
use std::sync::Arc;

use coral_db::{Database, Transaction};
use coral_provider::LoadedGenerations;
use coral_telemetry::safe;
use coral_workspace::{AuthenticatedPrincipal, PrincipalWorkspaceEvidence};
use coral_workspace::{WorkspaceId, WorkspaceModule};

use page::{Ranked, hydrate_page_in_transaction};
use retriever::functions::SavedFunctionRetriever;
use retriever::tools::ToolRetriever;
use retriever::{
    Answer, EntityRetriever, QueryError, QueryRequest, RequestContext, WindowEntry, abandon_query,
    begin_query, count, end_query,
};

pub use discovery::{Discovery, DiscoveryRequest, ExactLookup, ExactPaths, MissingPath};
pub use error::Error;
pub use index::SearchIndex;
pub use storage::{StorageError, prepare_storage};
pub use suggestion::SUGGESTIONS_MAX;
pub use types::{
    ApprovalRequirement, AuthoringDescription, CallPath, EntityKind, ExactAuthoringDescription,
    ExactSavedFunctionDetail, ProviderCallRoot, ProviderId, RetrieverCoverage, RetrieverState,
    RetrieverStatus, SEARCH_DEFAULT_LIMIT, SEARCH_PAGE_MAX_BYTES, SEARCH_QUERY_MAX_BYTES,
    SEARCH_RESULT_LIMIT_MAX, SavedFunctionAuthoringDescription, SearchHit, SearchIndexFailure,
    SearchIndexState, SearchInput, SearchLimit, SearchPage, SearchQuery, SearchRequest,
    ToolAuthoringDescription, ToolHit, provider_filter_value,
};

/// How many ranked Search Entities each retriever hands the engine. The
/// fused union of the windows is the result universe an offset pages through.
const RETRIEVAL_WINDOW: usize = 100;

/// Transport-neutral Universal Search Module.
#[derive(Clone)]
pub struct UniversalSearchModule {
    database: Arc<Database>,
    workspaces: WorkspaceModule,
    /// One retriever per Search Entity kind. At equal mark and position, an
    /// earlier retriever's Entity comes first: Tools, then Saved Functions.
    /// A strong Saved Function therefore leads every Tool below its
    /// position, but not the strong Tool at its own.
    retrievers: Arc<[Arc<dyn EntityRetriever>]>,
}

/// The most type names one `describe` call may request.
pub const DESCRIBE_TYPES_MAX: usize = 32;
/// The longest type name, in UTF-8 bytes, one `describe` call may request.
pub const DESCRIBE_TYPE_NAME_MAX_BYTES: usize = 256;

impl UniversalSearchModule {
    /// Builds discovery over initialized domain persistence and Workspace authority.
    #[must_use]
    pub fn new(database: Arc<Database>, workspaces: WorkspaceModule) -> Self {
        Self::with_retrievers(
            database,
            workspaces,
            SavedFunctionRetriever::default(),
            ToolRetriever::default(),
        )
    }

    /// Builds discovery whose named retrievers fail to rank, through the
    /// same path a failing ranked statement takes.
    #[cfg(test)]
    fn with_failing_rankings(
        database: Arc<Database>,
        workspaces: WorkspaceModule,
        kinds: &[EntityKind],
    ) -> Self {
        Self::with_retrievers(
            database,
            workspaces,
            SavedFunctionRetriever {
                fail_ranking: kinds.contains(&EntityKind::SavedFunction),
            },
            ToolRetriever {
                fail_ranking: kinds.contains(&EntityKind::ProviderTool),
            },
        )
    }

    fn with_retrievers(
        database: Arc<Database>,
        workspaces: WorkspaceModule,
        functions: SavedFunctionRetriever,
        tools: ToolRetriever,
    ) -> Self {
        Self {
            database,
            workspaces,
            retrievers: Arc::new([
                Arc::new(tools) as Arc<dyn EntityRetriever>,
                Arc::new(functions),
            ]),
        }
    }

    /// Discovers what the caller can call: a ranked page of Search Hits for
    /// a query, or the exact descriptions of named Call Paths. Each request
    /// reads one snapshot, so a page and the descriptions it holds come from
    /// one catalog state.
    ///
    /// # Errors
    ///
    /// As [`Self::search`] for a query. For paths, authority and shared
    /// persistence failures, and [`Error::UndeclaredTypes`] when `types`
    /// names a type the one path does not declare. A path the caller cannot
    /// discover is missing, not an error.
    pub async fn discover(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        request: &DiscoveryRequest,
    ) -> Result<Discovery, Error> {
        match request {
            DiscoveryRequest::Query(request) => self
                .page(actor, workspace_id, request)
                .await
                .map(Discovery::Page),
            DiscoveryRequest::Paths(paths) => self
                .look_up(actor, workspace_id, paths)
                .await
                .map(Discovery::Exact),
        }
    }

    /// Searches visible Search Entities and returns a page of Search Hits:
    /// the query mode of [`Self::discover`].
    ///
    /// Each retriever applies current visibility and ranks its own kind; the
    /// engine fuses the windows by position, strong matches first. Only the
    /// page's items are hydrated, each into a hit with a signature line,
    /// snippets, and evidence of why it matched. Every page carries one
    /// status per retriever. An omitted query
    /// lists every visible Entity in Call Path order and reads no index.
    ///
    /// # Errors
    ///
    /// Returns authority or shared persistence failures, and
    /// [`Error::ServiceUnavailable`] when every applied retriever fails to rank.
    pub async fn search(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        request: &SearchRequest,
    ) -> Result<SearchPage, Error> {
        self.page(actor, workspace_id, request).await
    }

    async fn page(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        request: &SearchRequest,
    ) -> Result<SearchPage, Error> {
        let terms = request
            .search_query()
            .map(|query| query_terms(query.as_str()));

        let mut transaction = self.database.begin_read_snapshot().await?;
        let authority = self
            .authorize(&mut transaction, actor, workspace_id)
            .await?;
        let loaded = LoadedGenerations::default();
        let context = RequestContext {
            actor,
            authority: &authority,
            workspace_id,
            loaded: &loaded,
        };
        let (ranked, statuses) = match &terms {
            None => {
                self.list_in_transaction(&mut transaction, context, request.provider())
                    .await?
            }
            Some(terms) => {
                self.rank_in_transaction(
                    &mut transaction,
                    context,
                    &QueryRequest {
                        terms,
                        provider_filter: request.provider(),
                        window: RETRIEVAL_WINDOW,
                    },
                )
                .await?
            }
        };
        let page = hydrate_page_in_transaction(
            &mut transaction,
            context,
            &self.retrievers,
            &ranked,
            usize::try_from(request.offset())
                .map_or(ranked.len(), |offset| offset.min(ranked.len())),
            request.limit(),
            statuses,
        )
        .await?;
        transaction.commit().await?;
        Ok(page)
    }

    /// The Call Roots of the Providers the caller can search now, in order:
    /// the values a search's Provider filter accepts. Reads no index.
    ///
    /// It needs the authority to inspect the Workspace, as a Task start
    /// does, not discovery admission: a Task still starts in an archived
    /// Workspace, though its searches are refused.
    ///
    /// # Errors
    ///
    /// Returns authority or shared persistence failures.
    pub async fn searchable_providers(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<ProviderCallRoot>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let authority = self
            .workspaces
            .check_principal_inspection_in_snapshot(
                &mut transaction,
                actor,
                workspace_id,
                now_unix_nanos()?,
            )
            .await?;
        let call_roots = retriever::tools::searchable_call_roots(
            &mut transaction,
            RequestContext {
                actor,
                authority: &authority,
                workspace_id,
                loaded: &LoadedGenerations::default(),
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(call_roots)
    }

    /// Lists every visible Entity of every kind in Call Path order.
    async fn list_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        provider_filter: Option<&ProviderCallRoot>,
    ) -> Result<(Vec<Ranked>, Vec<RetrieverStatus>), Error> {
        let mut listed = Vec::new();
        let mut statuses = Vec::with_capacity(self.retrievers.len());
        for (retriever_index, retriever) in self.retrievers.iter().enumerate() {
            let listing = retriever
                .list(transaction, context, provider_filter)
                .await?;
            let units = count(listing.units);
            let coverage =
                RetrieverCoverage::new(units, units, count(listing.entities.len()), false);
            listed.extend(listing.entities.into_iter().map(|entity| Ranked {
                retriever_index,
                entity,
            }));
            statuses.push(ranked_status(retriever.kind(), coverage));
        }
        listed.sort_by(|left, right| left.entity.call_path().cmp(right.entity.call_path()));
        Ok((listed, statuses))
    }

    /// Asks every retriever for its ranked window and fuses them.
    ///
    /// Each retriever runs inside a savepoint, so a failed ranked statement
    /// rolls back to it and the snapshot stays usable for the others. A
    /// retriever error becomes that retriever's `error` status; a shared
    /// failure fails the request.
    async fn rank_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        request: &QueryRequest<'_>,
    ) -> Result<(Vec<Ranked>, Vec<RetrieverStatus>), Error> {
        let mut windows = Vec::with_capacity(self.retrievers.len());
        let mut statuses = Vec::with_capacity(self.retrievers.len());
        let mut applied = 0usize;
        let mut failed = 0usize;
        for (retriever_index, retriever) in self.retrievers.iter().enumerate() {
            begin_query(transaction).await?;
            let answer = retriever.query(transaction, context, request).await;
            let status = match answer {
                Ok(Answer { window, coverage }) => {
                    end_query(transaction).await?;
                    applied += 1;
                    windows.push((retriever_index, window));
                    ranked_status(retriever.kind(), coverage)
                }
                Err(QueryError::Retriever(error)) => {
                    abandon_query(transaction).await?;
                    applied += 1;
                    failed += 1;
                    // TODO: attach an error monitor to failed rankings.
                    tracing::error!(
                        coral.search.retriever = safe::kind(retriever.kind().as_str()),
                        coral.workspace.id = safe::id(&context.workspace_id),
                        error.type = safe::kind(error.code()),
                        "a Universal Search retriever could not rank"
                    );
                    RetrieverStatus::new(
                        retriever.kind(),
                        RetrieverState::Error,
                        RetrieverCoverage::default(),
                    )
                }
                Err(QueryError::Shared(error)) => return Err(error),
            };
            statuses.push(status);
        }
        if applied > 0 && failed == applied {
            return Err(Error::ServiceUnavailable);
        }
        Ok((fuse(windows), statuses))
    }

    /// Describes one exact complete current Call Path independently of
    /// relevance health: the one-path mode of [`Self::discover`].
    ///
    /// The declaration is abridged. With `types`, it holds those types of the
    /// declaration instead, with their own nested types abridged the same
    /// way. Exact description of the User's own Saved Function additionally
    /// carries current source and saved diagnostics, except with `types`. An
    /// absent or invisible path is concealed as missing; authoritative
    /// corruption remains operational.
    ///
    /// # Errors
    ///
    /// Returns [`Error::AuthoringDescriptionNotFound`] for concealed absence,
    /// [`Error::TypesLimit`] when `types` has more than [`DESCRIBE_TYPES_MAX`]
    /// names or a name longer than [`DESCRIBE_TYPE_NAME_MAX_BYTES`],
    /// [`Error::UndeclaredTypes`] when it names a type the declaration does
    /// not declare, plus safe operational persistence or
    /// authoritative-description failures.
    pub async fn describe(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        call_path: &CallPath,
        types: &[String],
    ) -> Result<ExactAuthoringDescription, Error> {
        let paths = ExactPaths::one(call_path, types)?;
        self.look_up(actor, workspace_id, &paths)
            .await
            .map_err(conceal_description_error)?
            .descriptions
            .into_iter()
            .next()
            .ok_or(Error::AuthoringDescriptionNotFound)
    }

    /// Describes each named path the caller can discover, in one read
    /// snapshot, and reports the rest missing. A path that does not parse
    /// is missing too: it names nothing.
    async fn look_up(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        paths: &ExactPaths,
    ) -> Result<ExactLookup, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let authority = self
            .authorize(&mut transaction, actor, workspace_id)
            .await?;
        let mut found = Vec::with_capacity(paths.paths().len());
        let mut missing = Vec::new();
        let loaded = LoadedGenerations::default();
        let context = RequestContext {
            actor,
            authority: &authority,
            workspace_id,
            loaded: &loaded,
        };
        for written in paths.paths() {
            let exact = match CallPath::parse(written) {
                Ok(call_path) => self
                    .describe_in_transaction(
                        &mut transaction,
                        actor,
                        &authority,
                        workspace_id,
                        &call_path,
                    )
                    .await
                    .map_err(conceal_description_error),
                Err(_unparsable) => Ok(None),
            };
            match exact {
                Ok(Some(exact)) => found.push(exact),
                Ok(None) | Err(Error::AuthoringDescriptionNotFound) => {
                    let suggestions = suggestion::suggestions(
                        &mut transaction,
                        context,
                        &self.retrievers,
                        written,
                    )
                    .await?;
                    missing.push(MissingPath::new(written.clone(), suggestions));
                }
                Err(error) => return Err(error),
            }
        }
        transaction.commit().await?;
        let descriptions = found
            .into_iter()
            .map(|exact| exact.abridge(paths.types()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ExactLookup {
            descriptions,
            missing,
        })
    }

    /// Checks exact owning-resource visibility before an attributed attempt begins.
    /// This retains no snapshot and grants no later authority. Workspace archival
    /// does not conceal retained resources; discovery admission is checked separately.
    /// # Errors
    /// Conceals missing or unavailable descriptions; rechecks current caller authority.
    pub async fn check_description_visibility(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        call_path: &CallPath,
    ) -> Result<(), Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let authority = self
            .workspaces
            .check_principal_inspection_in_snapshot(
                &mut transaction,
                actor,
                workspace_id,
                now_unix_nanos()?,
            )
            .await
            .map_err(Error::from)
            .map_err(conceal_description_error)?;
        self.describe_in_transaction(&mut transaction, actor, &authority, workspace_id, call_path)
            .await?
            .ok_or(Error::AuthoringDescriptionNotFound)?;
        transaction.commit().await?;
        Ok(())
    }

    /// Describes each Tool in `call_paths` that the caller can describe,
    /// in one read snapshot.
    ///
    /// `describe`'s visibility rules apply. Every other path is omitted, and a
    /// caller without discovery authority reads nothing, so absence never says
    /// why. The descriptions are not abridged.
    ///
    /// # Errors
    ///
    /// Returns safe operational persistence or authoritative-description
    /// failures.
    pub async fn describe_tools(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        call_paths: &[CallPath],
    ) -> Result<Vec<ToolAuthoringDescription>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let authority = match self
            .authorize(&mut transaction, actor, workspace_id)
            .await
            .map_err(conceal_description_error)
        {
            Ok(authority) => authority,
            Err(Error::AuthoringDescriptionNotFound) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let loaded = LoadedGenerations::default();
        let context = RequestContext {
            actor,
            authority: &authority,
            workspace_id,
            loaded: &loaded,
        };
        let descriptions =
            retriever::tools::describe_tools(&mut transaction, context, call_paths).await?;
        transaction.commit().await?;
        Ok(descriptions)
    }

    /// Asks each retriever in turn for the exact path. Reads no index.
    async fn describe_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        authority: &PrincipalWorkspaceEvidence,
        workspace_id: &WorkspaceId,
        call_path: &CallPath,
    ) -> Result<Option<ExactAuthoringDescription>, Error> {
        let loaded = LoadedGenerations::default();
        let context = RequestContext {
            actor,
            authority,
            workspace_id,
            loaded: &loaded,
        };
        for retriever in self.retrievers.iter() {
            if let Some(exact) = retriever.describe(transaction, context, call_path).await? {
                return Ok(Some(exact));
            }
        }
        Ok(None)
    }

    async fn authorize(
        &self,
        transaction: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        Ok(self
            .workspaces
            .check_discovery_in_transaction(transaction, actor, workspace_id, now_unix_nanos()?)
            .await?)
    }
}

/// The terms every retriever gets. Trims, collapses whitespace, and lowercases
/// the query, then splits it into terms on every character that is not
/// alphanumeric or one of `_ - . # / @`. The whole query is one more term.
/// Terms are sorted and deduplicated.
fn query_terms(query: &str) -> Vec<String> {
    let text = query
        .split_whitespace()
        .map(str::to_lowercase)
        .collect::<Vec<_>>()
        .join(" ");
    let mut terms = text
        .split(|character: char| {
            !(character.is_alphanumeric() || matches!(character, '_' | '-' | '.' | '#' | '/' | '@'))
        })
        .filter(|term| !term.is_empty())
        .map(str::to_string)
        .chain(std::iter::once(text.clone()))
        .collect::<Vec<_>>();
    terms.sort();
    terms.dedup();
    terms
}

/// Fuses the retrievers' windows, each `(retriever index, window)` in
/// retriever order: the strong entries of every window by position, then the
/// weak ones by position. At equal position the earlier retriever comes
/// first. Each retriever marks its own entries by its own rule and signals,
/// so only a position and that mark cross into the engine, and no score is
/// compared across kinds. A weak entry is demoted below every strong one,
/// never dropped.
fn fuse(windows: Vec<(usize, Vec<WindowEntry>)>) -> Vec<Ranked> {
    let mut entries = windows
        .into_iter()
        .flat_map(|(retriever_index, window)| {
            window
                .into_iter()
                .enumerate()
                .map(move |(position, entry)| (!entry.strong, position, retriever_index, entry))
        })
        .collect::<Vec<_>>();
    entries.sort_by_key(|(weak, position, retriever_index, _entry)| {
        (*weak, *position, *retriever_index)
    });
    entries
        .into_iter()
        .map(|(_weak, _position, retriever_index, entry)| Ranked {
            retriever_index,
            entity: entry.entity,
        })
        .collect()
}

/// A ranked retriever's state follows from its coverage.
const fn ranked_status(retriever: EntityKind, coverage: RetrieverCoverage) -> RetrieverStatus {
    let state = if coverage.failed_units() > 0 {
        RetrieverState::Partial
    } else if coverage.returned_count() > 0 {
        RetrieverState::ResultsFound
    } else {
        RetrieverState::Empty
    };
    RetrieverStatus::new(retriever, state, coverage)
}

fn now_unix_nanos() -> Result<i64, Error> {
    unix_now_nanos().ok_or(Error::ServiceUnavailable)
}

/// The current time in Unix nanoseconds, or `None` when the system clock is
/// before 1970 or past the durable nanosecond range. Each caller turns `None`
/// into its own error.
pub(crate) fn unix_now_nanos() -> Option<i64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
}

fn conceal_description_error(error: Error) -> Error {
    match error {
        Error::PermissionDenied(_)
        | Error::Workspace(
            coral_workspace::Error::WorkspaceNotFound | coral_workspace::Error::PermissionDenied(_),
        ) => Error::AuthoringDescriptionNotFound,
        other => other,
    }
}

#[cfg(test)]
mod tests;
