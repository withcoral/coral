//! The write side of Universal Search: one [`EntityIndexer`] per Search
//! Entity kind, the sibling of the retriever that reads the same rows.
//!
//! An indexer writes the rows of its kind into the search-owned tables and
//! reads nothing but those tables. It joins the caller's transaction;
//! [`crate::SearchIndex`] owns when each transaction begins and ends.

pub(crate) mod functions;
mod repository;
mod retry;
pub(crate) mod tools;

pub(crate) use repository::generation_outcomes_in_workspace;

#[cfg(any(test, feature = "test-fixtures"))]
pub(crate) use repository::{generation_states_in_workspace, tool_rows_in_workspace};

use std::borrow::Cow;
use std::collections::HashSet;

use async_trait::async_trait;

use crate::SearchIndexFailure;
use crate::retriever::plan::split_words;
use coral_db::Transaction;
use coral_workspace::WorkspaceId;

/// The revision of the indexed text that this indexer writes. Raise it with
/// each change to the text or columns of a Tool or Saved Function row.
/// A reconcile pass then indexes again what an older indexer revision wrote.
///
/// 1: schema text holds each word once.
pub(crate) const INDEXER_REVISION: i64 = 1;

/// A failure to write the search index. The caller's transaction rolls back.
#[derive(Debug, thiserror::Error)]
pub(crate) enum IndexError {
    /// An entry cannot become a row, for example an invalid Call Path.
    #[error("invalid search entry: {0}")]
    InvalidEntry(String),
    /// Work on the index, such as preparing entries, did not finish within
    /// its limit and was rolled back.
    #[error("the index write did not finish within {} seconds", .0.as_secs())]
    TimedOut(std::time::Duration),
    /// The write ran past its limit while it held its Workspace's lock, and
    /// was rolled back.
    #[error(
        "the index write ran past its {}-second limit while it held the Workspace's lock",
        .0.as_secs()
    )]
    Overran(std::time::Duration),
    /// The write waited past its limit to begin or to take its Workspace's
    /// lock, behind another writer, and was rolled back.
    #[error(
        "the index write waited more than {} seconds for another writer",
        .0.as_secs()
    )]
    WriterBusy(std::time::Duration),
    /// The system clock cannot stamp the attempt.
    #[error("system clock is before 1970 or past the durable nanosecond range")]
    Clock,
    /// A search-owned row holds a value its column does not admit.
    #[error("corrupt search index: {0}")]
    Corrupt(&'static str),
    /// Reading the Providers' active Generations or preparing one failed.
    #[error(transparent)]
    Provider(#[from] coral_provider::Error),
    /// Reading the current Saved Functions failed.
    #[error(transparent)]
    SavedFunctions(#[from] coral_functions::Error),
    /// Database access failed.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
}

impl IndexError {
    /// The error's kind, for a span's `error.type`.
    pub(crate) const fn code(&self) -> &'static str {
        match self {
            Self::InvalidEntry(_) => "invalid_entry",
            Self::TimedOut(_) => "timed_out",
            Self::Overran(_) => "overran",
            Self::WriterBusy(_) => "writer_busy",
            Self::Clock => "clock",
            Self::Corrupt(_) => "corrupt",
            Self::Provider(_) => "provider",
            Self::SavedFunctions(_) => "saved_functions",
            Self::Database(_) => "database",
        }
    }

    /// The kind a failed Generation attempt records. A Saved Function error
    /// never fails a Generation.
    pub(crate) const fn failure(&self) -> Option<SearchIndexFailure> {
        match self {
            Self::InvalidEntry(_) => Some(SearchIndexFailure::InvalidEntry),
            Self::TimedOut(_) | Self::Overran(_) | Self::WriterBusy(_) => {
                Some(SearchIndexFailure::TimedOut)
            }
            Self::Clock => Some(SearchIndexFailure::Clock),
            Self::Corrupt(_) => Some(SearchIndexFailure::Corrupt),
            Self::Provider(_) => Some(SearchIndexFailure::Provider),
            Self::Database(_) => Some(SearchIndexFailure::Database),
            Self::SavedFunctions(_) => None,
        }
    }

    /// Whether the same attempt fails the same way until something changes:
    /// an entry that cannot become a row, or a write that ran past its limit
    /// while it held its Workspace's lock. Each other error may pass,
    /// including a preparation that ran past its limit: it may have waited
    /// for a database connection.
    pub(super) const fn repeats_until_change(&self) -> bool {
        matches!(self, Self::InvalidEntry(_) | Self::Overran(_))
    }
}

/// Evidence emitted only after the caller commits the index transaction.
pub(crate) enum IndexEffect {
    Updated,
    FirstVisible {
        published_at_unix_nanos: i64,
        path: coral_telemetry::metric::VisibilityPath,
    },
    Recovered {
        unavailable_since_unix_nanos: i64,
    },
}

/// Writes one Search Entity kind into the search-owned tables.
#[async_trait]
pub(crate) trait EntityIndexer: Send + Sync {
    /// What a notification hands over, prepared by the owner.
    type Entry: Send + Sync;
    /// The rows an entry becomes, without the stamp the write gives them.
    type Rows<'e>: Send + Sync
    where
        Self: 'e;
    /// What a removal names; `Infallible` when a database cascade removes.
    type Removal: Send + Sync;
    /// What the reconcile task compares the index against.
    type Expected: Send + Sync;

    /// The Workspace whose lock the caller takes, with
    /// [`lock_workspace`], before it writes the entry.
    fn workspace_id<'e>(&self, entry: &'e Self::Entry) -> &'e WorkspaceId;

    /// Builds the entry's rows. The caller builds them before it begins the
    /// write: on `SQLite` the write holds the one writer of the process, so
    /// the text work must run before it.
    fn rows<'e>(&self, entry: &'e Self::Entry) -> Result<Self::Rows<'e>, IndexError>;

    /// Writes `rows`, the entry's rows, and the entry's state, all or nothing
    /// within the transaction, and stamps each row with the time it writes
    /// them. Writing the same entry again leaves the same rows. The caller
    /// holds the entry's Workspace lock, so the stamp is later than each
    /// stamp before it while the clock moves forward, and the write's
    /// statistics count sees every write of that Workspace before it.
    async fn index(
        &self,
        transaction: &mut Transaction<'_>,
        entry: &Self::Entry,
        rows: &Self::Rows<'_>,
    ) -> Result<IndexEffect, IndexError>;

    /// Removes the rows and state the removal names.
    async fn remove(
        &self,
        transaction: &mut Transaction<'_>,
        removal: &Self::Removal,
    ) -> Result<(), IndexError>;

    /// Compares the index with `expected`, everything the owners hold now,
    /// and returns what `expected` holds and the index lacks, holds stale, or
    /// holds as failed; a failure whose retry rule is not due at
    /// `now_unix_nanos` is left for a later pass. Reads only, so a read
    /// snapshot serves it.
    ///
    /// The caller indexes each returned item in a transaction of its own, so
    /// one failure stays with its item. Once those writes commit, the same
    /// comparison returns nothing.
    async fn reconcile<'e>(
        &self,
        transaction: &mut Transaction<'_>,
        expected: &'e [Self::Expected],
        now_unix_nanos: i64,
    ) -> Result<Vec<&'e Self::Expected>, IndexError>;
}

/// The Workspaces a reconcile pass compared, and those among them whose
/// Postgres statistics it must count again.
pub(crate) struct StatisticsCheck {
    /// The number of Workspaces that have rows or statistics.
    pub(crate) workspaces_checked: usize,
    /// The Workspaces whose rows changed after their last count, or whose
    /// statistics no count with a watermark wrote.
    pub(crate) stale: Vec<WorkspaceId>,
}

/// Compares each Workspace's statistics watermark with its rows. Each
/// change of a Workspace's rows changes its row count or its newest stamp: an
/// index write stamps each row it changes with the time it took the
/// Workspace's lock, later than each stamp before it while the clock moves
/// forward, and a delete lowers the count. `SQLite` keeps no statistics, so
/// it has none to check.
pub(crate) async fn check_statistics(
    transaction: &mut Transaction<'_>,
) -> Result<StatisticsCheck, IndexError> {
    let watermarks = repository::statistics_watermarks(transaction).await?;
    let workspaces_checked = watermarks.len();
    let stale = watermarks
        .into_iter()
        .filter(|workspace| workspace.counted != Some(workspace.current))
        .map(|workspace| workspace.workspace_id)
        .collect();
    Ok(StatisticsCheck {
        workspaces_checked,
        stale,
    })
}

/// On Postgres, takes the transaction-scoped lock on the Workspace's search
/// index writes; `SQLite` serializes every write already.
pub(crate) async fn lock_workspace(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<(), IndexError> {
    Ok(repository::lock_workspace(transaction, workspace_id).await?)
}

/// On Postgres, takes the Workspace's lock and rewrites its ranking
/// statistics from its rows. Returns the statistics rows written or deleted.
pub(crate) async fn refresh_statistics(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<u64, IndexError> {
    repository::lock_workspace(transaction, workspace_id).await?;
    Ok(repository::refresh_workspace_statistics(transaction, workspace_id).await?)
}

fn now_unix_nanos() -> Result<i64, IndexError> {
    crate::unix_now_nanos().ok_or(IndexError::Clock)
}

/// Schema text with each word once. Words split as the query planner splits
/// a term, and compare without case. Each word keeps the spelling and the
/// place of its first occurrence. Spaces join the words.
///
/// A shared type that many paths reach repeats its words under each path,
/// so a large schema's text is mostly repeats. A repeat adds no match: both
/// backends still find each word and each substring inside a word.
#[derive(Default)]
pub(crate) struct DistinctWords<'a> {
    /// The lowercase form of each word kept.
    seen: HashSet<Cow<'a, str>>,
    /// Each line of field text pushed.
    lines: HashSet<&'a str>,
    text: String,
}

impl<'a> DistinctWords<'a> {
    /// Pushes one declaration field: its path, then each line of its text.
    /// The text usually opens with the path, and a shared type repeats its
    /// lines under each path that reaches it. A line pushed before holds no
    /// new word, so it is not split again.
    pub(crate) fn push_field(&mut self, path: &'a str, text: &'a str) {
        self.push(path);
        for line in text.lines() {
            if line != path && self.lines.insert(line) {
                self.push(line);
            }
        }
    }

    pub(crate) fn push(&mut self, piece: &'a str) {
        for word in split_words(piece) {
            // An ASCII word without a capital is its own lowercase form, so
            // most words need no copy.
            let key = if word.is_ascii() && !word.bytes().any(|byte| byte.is_ascii_uppercase()) {
                Cow::Borrowed(word)
            } else {
                Cow::Owned(word.to_lowercase())
            };
            if self.seen.insert(key) {
                if !self.text.is_empty() {
                    self.text.push(' ');
                }
                self.text.push_str(word);
            }
        }
    }

    pub(crate) fn into_text(self) -> String {
        self.text
    }
}

/// The compact identifier variant of a name: lowercase, every character that
/// is not alphanumeric removed. `deploy_url` gives `deployurl`. `None` when
/// the variant adds nothing. Indexed beside the name, it gives a compact
/// query variant a whole lexeme to meet. Retrievers derive the query's
/// variants with the same rule.
pub(crate) fn compact_identifier_variant(value: &str) -> Option<String> {
    let normalized = value.trim().to_lowercase();
    let compact = normalized
        .chars()
        .filter(|character| character.is_alphanumeric())
        .collect::<String>();
    (!compact.is_empty() && compact != normalized).then_some(compact)
}

/// A snapshot of projection work, distinct from catalogs awaiting first visibility.
pub(crate) struct ProjectionBacklog {
    pub catalogs: u64,
    pub functions: u64,
    /// None when a legacy failed row has unknown visibility history.
    pub first_visibility: Option<Vec<coral_catalog::ProviderGenerationId>>,
}

pub(crate) async fn projection_backlog(
    transaction: &mut Transaction<'_>,
    providers: &[coral_provider::ActiveGeneration],
    functions: &[coral_functions::SavedFunctionIndexRevision],
) -> Result<ProjectionBacklog, IndexError> {
    let generations = repository::recorded_generations(transaction)
        .await?
        .into_iter()
        .map(|state| (state.generation.clone(), state))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut catalogs = 0;
    let mut first_visibility = Some(Vec::new());
    for provider in providers {
        let key = repository::WorkspaceGeneration {
            workspace_id: provider.workspace_id().clone(),
            generation_id: provider.generation_id().clone(),
        };
        let state = generations.get(&key);
        if state.is_some_and(|state| tools::current_projection(state, provider)) {
            continue;
        }
        catalogs += 1;
        match state.map(|state| state.visibility) {
            None | Some(repository::RecordedVisibility::NeverVisible) => {
                if let Some(missing) = &mut first_visibility {
                    missing.push(provider.generation_id().clone());
                }
            }
            Some(repository::RecordedVisibility::Unknown) => first_visibility = None,
            Some(repository::RecordedVisibility::Visible { .. }) => {}
        }
    }
    let recorded = repository::recorded_functions(transaction)
        .await?
        .into_iter()
        .map(|row| (row.function, row.fingerprint))
        .collect::<std::collections::HashMap<_, _>>();
    let missing_functions = functions
        .iter()
        .filter(|entry| {
            let key = repository::WorkspaceFunction {
                workspace_id: entry.workspace_id().clone(),
                function_id: entry.id().clone(),
            };
            recorded.get(&key) != Some(&repository::function_fingerprint(entry.revision()))
        })
        .count();
    Ok(ProjectionBacklog {
        catalogs,
        functions: u64::try_from(missing_functions).unwrap_or(u64::MAX),
        first_visibility,
    })
}
